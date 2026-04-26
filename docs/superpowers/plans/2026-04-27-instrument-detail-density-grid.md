# Instrument detail — density grid + filings rendering

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the current stacked Research-tab layout + thin 10-K drilldown with a Bloomberg-style density grid, three-pane 10-K reader (TOC + reader + metadata rail with prior 10-Ks + cross-ref hover popovers + inline embedded tables), and a filterable 8-K page.

**Architecture:** Four independent PRs. Phase 1 changes the parser + adds a `tables_json JSONB` column on `instrument_business_summary_sections`; Phase 2 rebuilds the 10-K drilldown frontend + adds an `?accession=` query param + a sibling history endpoint; Phase 3 swaps the Research tab to a grid and adds a 5-row Filings pane that links to the drilldowns; Phase 4 adds an 8-K filterable detail route. Phase 1 unblocks Phase 2; Phases 3 and 4 are independent of each other after Phase 2.

**Tech Stack:** FastAPI + psycopg3 + pyright (backend); React 18 + TypeScript + Tailwind + react-router-dom + `useAsync` (frontend); Pytest + Vitest.

**Spec:** `docs/superpowers/specs/2026-04-27-instrument-detail-density-grid-design.md`

---

## File Structure

### Phase 1 — schema + parser

| Path | Action | Responsibility |
|---|---|---|
| `sql/075_business_sections_tables_json.sql` | Create | Add nullable `tables_json JSONB` column on `instrument_business_summary_sections` |
| `app/services/business_summary.py` | Modify | Add `ParsedTable` dataclass, sentinel-substitute `<table>` blocks, persist `tables_json` |
| `app/api/instruments.py` | Modify | Extend `BusinessSectionModel` with `tables: list[BusinessTableModel]` |
| `tests/services/test_business_summary_tables.py` | Create | Round-trip parser test for embedded tables |
| `tests/api/test_instruments_business_sections_endpoint.py` | Create | API contract test asserting `tables` field present |

### Phase 2 — 10-K drilldown rebuild

| Path | Action | Responsibility |
|---|---|---|
| `app/api/instruments.py` | Modify | `?accession=` query param on `/business_sections`; new `/filings/10-k/history` endpoint |
| `app/services/business_summary.py` | Modify | `get_business_sections` accepts optional `accession`; new `list_10k_history` |
| `frontend/src/api/instruments.ts` | Modify | `fetchBusinessSections` accession arg; new `fetchTenKHistory` |
| `frontend/src/pages/Tenk10KDrilldownPage.tsx` | Rewrite | Three-pane full-width layout; continuous vertical line; cross-ref popover; embedded tables |
| `frontend/src/components/instrument/CrossRefPopover.tsx` | Create | 240-char excerpt + "Open full" link popover |
| `frontend/src/components/instrument/EmbeddedTable.tsx` | Create | Renders one `BusinessTable` payload as `<table>` |
| `frontend/src/components/instrument/TenKMetadataRail.tsx` | Create | Right rail: filing accession + prior 10-Ks list + related items |
| `frontend/src/pages/Tenk10KDrilldownPage.test.tsx` | Create | Vitest covering three-pane render + popover + table render |
| `tests/api/test_instruments_tenk_history_endpoint.py` | Create | History endpoint contract test |

### Phase 3 — instrument page density grid

| Path | Action | Responsibility |
|---|---|---|
| `frontend/src/components/instrument/DensityGrid.tsx` | Create | Grid container — chart 2×2 + 6 panes |
| `frontend/src/components/instrument/FilingsPane.tsx` | Create | 5-row filings list (8-K + 10-K) linking to drilldowns |
| `frontend/src/components/instrument/ResearchTab.tsx` | Rewrite | Render `<DensityGrid>` instead of stacked panels |
| `frontend/src/pages/InstrumentPage.tsx` | Modify | Move `<PriceChart>` into the grid (no longer top-of-tab) |
| `frontend/src/components/instrument/DensityGrid.test.tsx` | Create | Vitest covering pane order + responsive collapse |
| `frontend/src/components/instrument/FilingsPane.test.tsx` | Create | Vitest covering 5-row cap + click-routes |

### Phase 4 — 8-K filterable detail page

| Path | Action | Responsibility |
|---|---|---|
| `frontend/src/pages/EightKListPage.tsx` | Create | Route `/instrument/:symbol/filings/8-k` — table + filter strip + detail panel |
| `frontend/src/components/instrument/EightKFilterStrip.tsx` | Create | Severity / item-code / date-range filter controls |
| `frontend/src/components/instrument/EightKDetailPanel.tsx` | Create | Detail panel for selected row |
| `frontend/src/App.tsx` | Modify | Register the new route |
| `frontend/src/components/instrument/FilingsPane.tsx` | Modify | Wire 8-K row clicks to `/filings/8-k?accession=...` |
| `frontend/src/pages/EightKListPage.test.tsx` | Create | Vitest covering filter + selection + URL deep-link |

---

## Testing strategy

- **Backend:** TDD per task. Every parser/service/endpoint change ships with a failing test first, then implementation. Existing fixtures under `tests/fixtures/` (notably the GME 10-K) drive integration coverage.
- **Frontend:** Vitest unit tests per component. Each new page gets one happy-path + one filter/empty-state test. Visual / E2E left out — the spec calls a snapshot regression as nice-to-have.
- **Migration safety:** `tables_json` is nullable so the migration is reversible without a backfill. Backfill runs as the final step of Phase 1 via `bootstrap_business_summaries`.
- **Smoke test:** `tests/smoke/test_app_boots.py` already covers app boot — touch nothing there.

---

# Phase 1 — schema + parser

**Branch:** `feature/559-phase1-tables-json`

**Goal:** Persist embedded `<table>` blocks from 10-K Item 1 prose so the drilldown can render them as real tables instead of stripped whitespace.

## Task 1.1: Migration — `tables_json` column

**Files:**
- Create: `sql/075_business_sections_tables_json.sql`
- Test: manual `psql` check

- [ ] **Step 1: Write the migration**

```sql
-- 075_business_sections_tables_json.sql
--
-- #559 Phase 1: persist embedded <table> blocks from 10-K Item 1
-- prose so the renderer can show them as real tables instead of
-- stripped whitespace runs.
--
-- Nullable column. Existing rows stay NULL until the next parse
-- via bootstrap_business_summaries (post-deploy).
ALTER TABLE instrument_business_summary_sections
    ADD COLUMN IF NOT EXISTS tables_json JSONB;

COMMENT ON COLUMN instrument_business_summary_sections.tables_json IS
    'Array of {order:int, headers:[str], rows:[[str]]} for embedded '
    '<table> blocks parsed from this section. NULL = not yet re-parsed; '
    'empty array = re-parsed and section had no tables.';
```

- [ ] **Step 2: Apply migration locally**

Run: `uv run python -m app.migrate 075`
Expected: `applied 075_business_sections_tables_json.sql`

- [ ] **Step 3: Verify column exists**

Run: `psql $DATABASE_URL -c "\d instrument_business_summary_sections"`
Expected: `tables_json | jsonb` row in the column list.

- [ ] **Step 4: Commit**

```bash
git checkout -b feature/559-phase1-tables-json
git add sql/075_business_sections_tables_json.sql
git commit -m "feat(#559-phase1): tables_json column for embedded 10-K tables"
```

## Task 1.2: Parser — `ParsedTable` dataclass + sentinel substitution (failing test)

**Files:**
- Modify: `app/services/business_summary.py`
- Test: `tests/services/test_business_summary_tables.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/services/test_business_summary_tables.py
"""Parser must preserve <table> blocks as structured payloads (#559)."""
from app.services.business_summary import (
    ParsedTable,
    extract_business_sections,
)


def test_table_block_extracted_as_parsed_table():
    raw = """
    <html><body>
    <p>Item 1. Business</p>
    <p>As of January 31, 2026 we operated 2,206 stores:</p>
    <table>
      <tr><th>Segment</th><th>Stores</th></tr>
      <tr><td>United States</td><td>1,598</td></tr>
      <tr><td>Europe</td><td>308</td></tr>
      <tr><td>Australia</td><td>300</td></tr>
    </table>
    <p>Our stores operate primarily under GameStop brands.</p>
    <p>Item 1A. Risk Factors</p>
    </body></html>
    """
    sections = extract_business_sections(raw)
    assert sections, "expected at least one section"
    s0 = sections[0]
    assert len(s0.tables) == 1
    table = s0.tables[0]
    assert isinstance(table, ParsedTable)
    assert table.headers == ("Segment", "Stores")
    assert table.rows == (
        ("United States", "1,598"),
        ("Europe", "308"),
        ("Australia", "300"),
    )
    assert "TABLE_0" in s0.body or "␞TABLE_0␞" in s0.body, (
        "body should retain a sentinel marking the table's insertion point"
    )


def test_section_with_no_tables_has_empty_tuple():
    raw = """
    <html><body>
    <p>Item 1. Business</p>
    <p>We sell video games.</p>
    <p>Item 1A. Risk Factors</p>
    </body></html>
    """
    sections = extract_business_sections(raw)
    assert sections
    assert sections[0].tables == ()
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `uv run pytest tests/services/test_business_summary_tables.py -v`
Expected: `ImportError: cannot import name 'ParsedTable'` or both tests FAIL.

- [ ] **Step 3: Implement `ParsedTable` + sentinel substitution**

In `app/services/business_summary.py`:

(a) Above the `ParsedBusinessSection` dataclass, add:

```python
_TABLE_SENTINEL = "␞"  # SYMBOL FOR RECORD SEPARATOR — never appears in 10-K prose
_TABLE_BLOCK_RE = re.compile(r"<table\b[^>]*>.*?</table\s*>", re.IGNORECASE | re.DOTALL)
_TR_RE = re.compile(r"<tr\b[^>]*>(.*?)</tr\s*>", re.IGNORECASE | re.DOTALL)
_CELL_RE = re.compile(r"<(?:t[hd])\b[^>]*>(.*?)</t[hd]\s*>", re.IGNORECASE | re.DOTALL)


@dataclass(frozen=True)
class ParsedTable:
    """One <table> block extracted from a section body.

    ``headers`` is the first row's cell contents (treated as headers
    even when the source uses <td> rather than <th> — many 10-K
    issuers do).  ``rows`` are subsequent rows. Cells are plain text
    after entity decode + tag strip.
    """

    order: int
    headers: tuple[str, ...]
    rows: tuple[tuple[str, ...], ...]


def _parse_table_html(table_html: str) -> ParsedTable | None:
    """Extract a single <table> block into a ParsedTable, or None
    when the table has zero data rows (a layout table with no real
    content)."""
    cells_per_row: list[tuple[str, ...]] = []
    for tr_match in _TR_RE.finditer(table_html):
        cells = tuple(
            _strip_html(cell).strip()
            for cell in _CELL_RE.findall(tr_match.group(1))
        )
        if any(c for c in cells):  # skip rows that strip to all empty
            cells_per_row.append(cells)
    if not cells_per_row:
        return None
    headers, *body_rows = cells_per_row
    return ParsedTable(
        order=0,  # caller assigns the final order
        headers=headers,
        rows=tuple(body_rows),
    )


def _extract_tables(raw_html: str) -> tuple[str, tuple[ParsedTable, ...]]:
    """Replace every <table> block in ``raw_html`` with a sentinel
    ``␞TABLE_N␞`` and return the rewritten HTML + the
    parsed tables in source order."""
    tables: list[ParsedTable] = []

    def _sub(m: re.Match[str]) -> str:
        parsed = _parse_table_html(m.group(0))
        if parsed is None:
            return " "  # drop layout-only tables
        order = len(tables)
        tables.append(
            ParsedTable(order=order, headers=parsed.headers, rows=parsed.rows)
        )
        return f" {_TABLE_SENTINEL}TABLE_{order}{_TABLE_SENTINEL} "

    rewritten = _TABLE_BLOCK_RE.sub(_sub, raw_html)
    return rewritten, tuple(tables)
```

(b) Extend `ParsedBusinessSection` with `tables`:

```python
@dataclass(frozen=True)
class ParsedBusinessSection:
    """One subsection extracted from Item 1."""

    section_order: int
    section_key: str
    section_label: str
    body: str
    cross_references: tuple[ParsedCrossReference, ...]
    tables: tuple[ParsedTable, ...] = ()
```

(c) In `extract_business_sections`, run table extraction BEFORE `_wrap_heading_tags` so the sentinel survives the existing strip pass. Replace the `marked_html = _wrap_heading_tags(raw_html)` line with:

```python
    table_stripped_html, all_tables = _extract_tables(raw_html)
    marked_html = _wrap_heading_tags(table_stripped_html)
```

(d) After the section list is built (just before the `return tuple(sections)` at the end of `extract_business_sections`), redistribute `all_tables` to whichever section's body contains its sentinel:

```python
    if all_tables:
        sections = _attach_tables(sections, all_tables)
    return tuple(sections)
```

(e) Add the helper above `extract_business_sections`:

```python
def _attach_tables(
    sections: list[ParsedBusinessSection],
    all_tables: tuple[ParsedTable, ...],
) -> list[ParsedBusinessSection]:
    """Walk each section body, find ``␞TABLE_N␞`` markers,
    and attach the matching ParsedTable. Re-numbers tables per
    section so the renderer can index by ``section.tables[order]``."""
    result: list[ParsedBusinessSection] = []
    for s in sections:
        attached: list[ParsedTable] = []
        body = s.body
        for table in all_tables:
            marker = f"{_TABLE_SENTINEL}TABLE_{table.order}{_TABLE_SENTINEL}"
            if marker in body:
                local_order = len(attached)
                attached.append(
                    ParsedTable(
                        order=local_order,
                        headers=table.headers,
                        rows=table.rows,
                    )
                )
                body = body.replace(
                    marker,
                    f"{_TABLE_SENTINEL}TABLE_{local_order}{_TABLE_SENTINEL}",
                )
        result.append(
            ParsedBusinessSection(
                section_order=s.section_order,
                section_key=s.section_key,
                section_label=s.section_label,
                body=body,
                cross_references=s.cross_references,
                tables=tuple(attached),
            )
        )
    return result
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `uv run pytest tests/services/test_business_summary_tables.py -v`
Expected: both tests PASS.

- [ ] **Step 5: Run the broader parser test suite to confirm no regressions**

Run: `uv run pytest tests/ -k business_summary -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add app/services/business_summary.py tests/services/test_business_summary_tables.py
git commit -m "feat(#559-phase1): parse embedded <table> blocks via sentinel substitution"
```

## Task 1.3: Persist `tables_json` on upsert + read on get

**Files:**
- Modify: `app/services/business_summary.py:732-794` (`upsert_business_sections`)
- Modify: `app/services/business_summary.py:885-949` (`BusinessSectionRow` + `get_business_sections`)
- Test: `tests/services/test_business_summary_tables.py` (extend)

- [ ] **Step 1: Write the failing test**

Append to `tests/services/test_business_summary_tables.py`:

```python
def test_upsert_persists_tables_json(conn):
    """Round-trip: parse → upsert → get returns the same tables."""
    from app.services.business_summary import (
        ParsedBusinessSection,
        ParsedTable,
        upsert_business_sections,
        get_business_sections,
    )

    instrument_id = _make_instrument(conn)
    sections = (
        ParsedBusinessSection(
            section_order=0,
            section_key="general",
            section_label="General",
            body="Body ␞TABLE_0␞ text.",
            cross_references=(),
            tables=(
                ParsedTable(
                    order=0,
                    headers=("Segment", "Stores"),
                    rows=(("US", "1598"), ("EU", "308")),
                ),
            ),
        ),
    )
    upsert_business_sections(
        conn,
        instrument_id=instrument_id,
        source_accession="0001326380-26-000001",
        sections=sections,
    )

    rows = get_business_sections(conn, instrument_id=instrument_id)
    assert len(rows) == 1
    assert len(rows[0].tables) == 1
    assert rows[0].tables[0].headers == ("Segment", "Stores")
    assert rows[0].tables[0].rows == (("US", "1598"), ("EU", "308"))
```

Add this fixture-helper at the top of the file (or import from `tests/fixtures/`):

```python
import pytest
import psycopg
from app.config import settings


@pytest.fixture()
def conn():
    """Per-test connection rolled back at teardown so we never
    contaminate the dev DB. Settings.database_url MUST point to the
    test DB — run via the existing tests/ harness."""
    with psycopg.connect(settings.database_url) as c:
        yield c
        c.rollback()


def _make_instrument(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            "INSERT INTO instruments (symbol, exchange) "
            "VALUES ('TEST559', 'NYSE') RETURNING instrument_id"
        )
        row = cur.fetchone()
        assert row
        return int(row[0])
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/services/test_business_summary_tables.py::test_upsert_persists_tables_json -v`
Expected: FAIL on `tables` not being a column / not being read back.

- [ ] **Step 3: Update `upsert_business_sections` to write `tables_json`**

In the INSERT block at `app/services/business_summary.py:776-792`, change:

```python
                cur.execute(
                    """
                    INSERT INTO instrument_business_summary_sections
                        (instrument_id, source_accession, section_order,
                         section_key, section_label, body, cross_references,
                         tables_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        instrument_id,
                        source_accession,
                        section.section_order,
                        section.section_key,
                        section.section_label,
                        section.body,
                        cross_refs_json,
                        Jsonb(
                            [
                                {
                                    "order": t.order,
                                    "headers": list(t.headers),
                                    "rows": [list(r) for r in t.rows],
                                }
                                for t in section.tables
                            ]
                        ),
                    ),
                )
```

- [ ] **Step 4: Update `BusinessSectionRow` + `get_business_sections` to read `tables_json`**

In `app/services/business_summary.py`:

```python
@dataclass(frozen=True)
class BusinessSectionRow:
    section_order: int
    section_key: str
    section_label: str
    body: str
    cross_references: tuple[ParsedCrossReference, ...]
    source_accession: str
    tables: tuple[ParsedTable, ...] = ()
```

In `get_business_sections`, change the SELECT to include `tables_json`:

```python
            SELECT section_order, section_key, section_label, body,
                   cross_references, source_accession, tables_json
            FROM instrument_business_summary_sections
            ...
```

And the row-build:

```python
        tables_raw = r[6] or []
        tables_list = tables_raw if isinstance(tables_raw, list) else []
        tables = tuple(
            ParsedTable(
                order=int(t.get("order", i)),
                headers=tuple(str(h) for h in t.get("headers", [])),
                rows=tuple(
                    tuple(str(c) for c in row) for row in t.get("rows", [])
                ),
            )
            for i, t in enumerate(tables_list)
            if isinstance(t, dict)
        )
        rows.append(
            BusinessSectionRow(
                section_order=int(r[0]),
                section_key=str(r[1]),
                section_label=str(r[2]),
                body=str(r[3]),
                cross_references=refs,
                source_accession=str(r[5]),
                tables=tables,
            )
        )
```

- [ ] **Step 5: Run to verify pass**

Run: `uv run pytest tests/services/test_business_summary_tables.py -v`
Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add app/services/business_summary.py tests/services/test_business_summary_tables.py
git commit -m "feat(#559-phase1): persist + read tables_json on business_sections"
```

## Task 1.4: API — surface `tables` on `/business_sections`

**Files:**
- Modify: `app/api/instruments.py:998-1024` (Business response models)
- Modify: `app/api/instruments.py:1026-1093` (endpoint body)
- Test: `tests/api/test_instruments_business_sections_endpoint.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/api/test_instruments_business_sections_endpoint.py
"""GET /instruments/{symbol}/business_sections must return tables (#559)."""
from fastapi.testclient import TestClient

from app.main import app


def test_business_sections_response_includes_tables_field():
    client = TestClient(app)
    # GME has tables in its 10-K Item 1 — pinned by Phase 1 backfill.
    r = client.get("/instruments/GME/business_sections")
    assert r.status_code == 200, r.text
    body = r.json()
    assert "sections" in body
    for s in body["sections"]:
        assert "tables" in s
        for t in s["tables"]:
            assert {"order", "headers", "rows"} <= set(t)
            assert isinstance(t["headers"], list)
            assert isinstance(t["rows"], list)
            for row in t["rows"]:
                assert isinstance(row, list)


def test_business_sections_tables_empty_for_section_without_tables():
    client = TestClient(app)
    r = client.get("/instruments/GME/business_sections")
    body = r.json()
    # At least one section must surface tables, at least one must not —
    # mixed coverage proves we're not falsifying.
    has_with = any(s["tables"] for s in body["sections"])
    has_without = any(not s["tables"] for s in body["sections"])
    assert has_with, "expected at least one section with tables (GME 10-K)"
    assert has_without, "expected at least one section with no tables"
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/api/test_instruments_business_sections_endpoint.py -v`
Expected: FAIL — `tables` field missing from response.

- [ ] **Step 3: Extend response models**

In `app/api/instruments.py` near the existing `BusinessSectionModel`:

```python
class BusinessTableModel(BaseModel):
    order: int
    headers: list[str]
    rows: list[list[str]]


class BusinessSectionModel(BaseModel):
    section_order: int
    section_key: str
    section_label: str
    body: str
    cross_references: list[BusinessCrossReferenceModel]
    tables: list[BusinessTableModel] = []
```

- [ ] **Step 4: Map `tables` in the endpoint body**

In `get_instrument_business_sections`, replace the existing list comprehension that builds `BusinessSectionModel` rows so each section includes:

```python
                tables=[
                    BusinessTableModel(
                        order=t.order,
                        headers=list(t.headers),
                        rows=[list(r) for r in t.rows],
                    )
                    for t in s.tables
                ],
```

- [ ] **Step 5: Run to verify pass**

Run: `uv run pytest tests/api/test_instruments_business_sections_endpoint.py -v`
Expected: PASS (after Phase 1 backfill — see Task 1.5).

- [ ] **Step 6: Commit**

```bash
git add app/api/instruments.py tests/api/test_instruments_business_sections_endpoint.py
git commit -m "feat(#559-phase1): expose tables on business_sections response"
```

## Task 1.5: Backfill — re-parse all SEC instruments

**Files:**
- Use: `app/services/business_summary.py::bootstrap_business_summaries` (no code change; existing function re-runs the parser end-to-end)
- Trigger: existing scheduler job at `app/workers/scheduler.py:3119` calls `bootstrap_business_summaries`.

The `bootstrap_business_summaries` function takes a `fetcher` and only re-parses when the candidate query returns the row. Existing rows with `source_accession != NULL` are *not* re-fetched/re-parsed by default. To force re-parse for the whole SEC-CIK universe, truncate the sections table (the blob `instrument_business_summary` body stays — only the per-section breakdown gets rebuilt).

- [ ] **Step 1: Snapshot section count before backfill**

Run: `psql $DATABASE_URL -c "SELECT COUNT(*) FROM instrument_business_summary_sections"`
Expected: integer; record it.

- [ ] **Step 2: Truncate sections table to force re-parse**

Run: `psql $DATABASE_URL -c "TRUNCATE TABLE instrument_business_summary_sections"`
Expected: `TRUNCATE TABLE`. The blob table is untouched, so the existing sections re-derive from the same cached HTML.

- [ ] **Step 3: Trigger the backfill via the scheduler job**

Open the admin sync UI (or curl) and trigger the existing `business_summaries_backfill` job — it calls `bootstrap_business_summaries` for the SEC provider. Tail logs:

Run: `curl -X POST $BACKEND/admin/sync/jobs/business_summaries_backfill/run`
Expected: 202 + `bootstrap_business_summaries complete: scanned=<n> inserted=<n>` log line within ~10–30 minutes (the SEC fair-use rate limit gates throughput).

(If the `business_summaries_backfill` job name differs, grep `app/workers/scheduler.py` for `bootstrap_business_summaries` to find the registered job id.)

- [ ] **Step 2: Spot-check GME**

Run: `psql $DATABASE_URL -c "SELECT section_label, jsonb_array_length(tables_json) AS n_tables FROM instrument_business_summary_sections WHERE instrument_id = (SELECT instrument_id FROM instruments WHERE symbol='GME' LIMIT 1) ORDER BY section_order"`
Expected: at least one row with `n_tables >= 1`.

- [ ] **Step 3: Run the full pre-push gate**

Run:
```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest
```
Expected: all four pass.

- [ ] **Step 4: Codex review on the branch (CLAUDE.md checkpoint 2 — before first push)**

Run: `codex.cmd exec review`
Expected: `OK` or actionable findings; fix anything real before pushing.

- [ ] **Step 5: Push + open PR**

```bash
git push -u origin feature/559-phase1-tables-json
gh pr create --title "feat(#559-phase1): tables_json column + parser table extraction" --body "$(cat <<'EOF'
## What

- Migration `075` adds nullable `tables_json JSONB` on `instrument_business_summary_sections`.
- Parser substitutes `<table>` blocks with `␞TABLE_N␞` sentinels before strip; reattaches per-section.
- API surfaces `tables: [{order, headers, rows}]` on `/instruments/{symbol}/business_sections`.
- Backfill via `bootstrap_business_summaries` on dev.

## Why

Phase 1 of the density-grid spec (`docs/superpowers/specs/2026-04-27-instrument-detail-density-grid-design.md`). Phase 2 needs structured tables to render in the 10-K drilldown.

## Test plan

- [ ] `tests/services/test_business_summary_tables.py` covers parser round-trip + upsert/get.
- [ ] `tests/api/test_instruments_business_sections_endpoint.py` covers API surface.
- [ ] Manual: `psql` count of GME sections with `n_tables >= 1`.
EOF
)"
```

- [ ] **Step 6: Poll for review + CI**

Run (in loop, per CLAUDE.md): `gh pr view <PR#> --comments` and `gh pr checks <PR#>`. Resolve every comment to FIXED / DEFERRED / REBUTTED before merging.

- [ ] **Step 7: Merge when APPROVE on latest commit + CI green**

```bash
gh pr merge <PR#> --squash --delete-branch
git checkout main && git pull
```

---

# Phase 2 — 10-K drilldown rebuild

**Branch:** `feature/559-phase2-tenk-drilldown`

**Goal:** Three-pane full-width 10-K reader with continuous vertical line, embedded tables, cross-ref hover popovers, and prior-10-Ks rail. Adds `?accession=` query param + new history endpoint.

## Task 2.1: Extend `get_business_sections` with optional accession

**Files:**
- Modify: `app/services/business_summary.py:897-950` (`get_business_sections`)
- Test: `tests/services/test_business_summary_accession.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/services/test_business_summary_accession.py
"""get_business_sections respects optional accession filter (#559)."""
import pytest
from app.services.business_summary import (
    ParsedBusinessSection,
    upsert_business_sections,
    get_business_sections,
)


def test_get_returns_latest_when_no_accession_provided(conn):
    iid = _make_instrument(conn, "TEST5592")
    upsert_business_sections(
        conn,
        instrument_id=iid,
        source_accession="acc-old",
        sections=(_section("Old body"),),
    )
    upsert_business_sections(
        conn,
        instrument_id=iid,
        source_accession="acc-new",
        sections=(_section("New body"),),
    )
    rows = get_business_sections(conn, instrument_id=iid)
    assert rows[0].body == "New body"
    assert rows[0].source_accession == "acc-new"


def test_get_with_accession_returns_that_filings_sections(conn):
    iid = _make_instrument(conn, "TEST5593")
    upsert_business_sections(
        conn,
        instrument_id=iid,
        source_accession="acc-old",
        sections=(_section("Old body"),),
    )
    upsert_business_sections(
        conn,
        instrument_id=iid,
        source_accession="acc-new",
        sections=(_section("New body"),),
    )
    rows = get_business_sections(
        conn, instrument_id=iid, accession="acc-old"
    )
    assert rows[0].body == "Old body"
    assert rows[0].source_accession == "acc-old"


def _section(body: str) -> ParsedBusinessSection:
    return ParsedBusinessSection(
        section_order=0,
        section_key="general",
        section_label="General",
        body=body,
        cross_references=(),
    )
```

(Reuse the `conn` + `_make_instrument` helpers from `test_business_summary_tables.py` — copy or import from a shared `tests/fixtures/business_summary.py`.)

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/services/test_business_summary_accession.py -v`
Expected: FAIL — `accession` keyword not accepted.

- [ ] **Step 3: Implement**

Change `get_business_sections` signature + query in `app/services/business_summary.py`:

```python
def get_business_sections(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    accession: str | None = None,
) -> tuple[BusinessSectionRow, ...]:
    """Return Item 1 subsections for an instrument in source order.

    ``accession=None`` → latest filing (the existing behaviour).
    ``accession="acc-..."`` → that exact filing's sections.

    Empty tuple when no sections match.
    """
    with conn.cursor() as cur:
        if accession is None:
            cur.execute(
                """
                SELECT section_order, section_key, section_label, body,
                       cross_references, source_accession, tables_json
                FROM instrument_business_summary_sections
                WHERE instrument_id = %s
                  AND source_accession = (
                      SELECT source_accession
                      FROM instrument_business_summary_sections
                      WHERE instrument_id = %s
                      ORDER BY fetched_at DESC
                      LIMIT 1
                  )
                ORDER BY section_order ASC
                """,
                (instrument_id, instrument_id),
            )
        else:
            cur.execute(
                """
                SELECT section_order, section_key, section_label, body,
                       cross_references, source_accession, tables_json
                FROM instrument_business_summary_sections
                WHERE instrument_id = %s AND source_accession = %s
                ORDER BY section_order ASC
                """,
                (instrument_id, accession),
            )
        raw_rows = cur.fetchall()
    # ... existing row-build unchanged ...
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/services/test_business_summary_accession.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git checkout -b feature/559-phase2-tenk-drilldown
git add app/services/business_summary.py tests/services/test_business_summary_accession.py
git commit -m "feat(#559-phase2): get_business_sections accepts optional accession"
```

## Task 2.2: Add `?accession=` to the existing endpoint

**Files:**
- Modify: `app/api/instruments.py:1026-1093`
- Test: extend `tests/api/test_instruments_business_sections_endpoint.py`

- [ ] **Step 1: Write the failing test**

Append to `tests/api/test_instruments_business_sections_endpoint.py`:

```python
def test_business_sections_with_accession_returns_that_filing():
    client = TestClient(app)
    # First fetch the latest accession
    latest = client.get("/instruments/GME/business_sections").json()
    acc = latest["source_accession"]
    assert acc, "GME should have a current 10-K accession"
    # Re-fetch by accession explicitly
    by_acc = client.get(
        f"/instruments/GME/business_sections?accession={acc}"
    ).json()
    assert by_acc["source_accession"] == acc
    assert by_acc["sections"], "non-empty sections expected"


def test_business_sections_unknown_accession_404():
    client = TestClient(app)
    r = client.get("/instruments/GME/business_sections?accession=does-not-exist")
    assert r.status_code == 404
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/api/test_instruments_business_sections_endpoint.py::test_business_sections_with_accession_returns_that_filing -v`
Expected: FAIL — query param ignored or 200 with latest filing.

- [ ] **Step 3: Implement**

In `app/api/instruments.py::get_instrument_business_sections`, add the query param + plumb through:

```python
from fastapi import Query


@router.get(
    "/{symbol}/business_sections",
    response_model=BusinessSectionsResponse,
)
def get_instrument_business_sections(
    symbol: str,
    accession: str | None = Query(
        default=None,
        description="Specific 10-K accession; omit for the latest filing.",
    ),
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> BusinessSectionsResponse:
    # ... existing instrument lookup unchanged ...
    sections = get_business_sections(
        conn, instrument_id=instrument_id, accession=accession
    )
    if accession is not None and not sections:
        raise HTTPException(
            status_code=404,
            detail=f"no 10-K sections for {symbol} accession {accession}",
        )
    # ... existing response build unchanged ...
```

- [ ] **Step 4: Run to verify pass**

Run: `uv run pytest tests/api/test_instruments_business_sections_endpoint.py -v`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add app/api/instruments.py tests/api/test_instruments_business_sections_endpoint.py
git commit -m "feat(#559-phase2): ?accession= query on business_sections endpoint"
```

## Task 2.3: New `/filings/10-k/history` endpoint

**Files:**
- Create: service helper `list_10k_history` in `app/services/business_summary.py`
- Modify: `app/api/instruments.py` (add route)
- Test: `tests/api/test_instruments_tenk_history_endpoint.py` (create)

- [ ] **Step 1: Write the failing test**

```python
# tests/api/test_instruments_tenk_history_endpoint.py
"""GET /instruments/{symbol}/filings/10-k/history (#559)."""
from fastapi.testclient import TestClient

from app.main import app


def test_tenk_history_returns_descending_filing_dates():
    client = TestClient(app)
    r = client.get("/instruments/GME/filings/10-k/history")
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["symbol"] == "GME"
    assert isinstance(body["filings"], list)
    assert body["filings"], "expected at least one 10-K"
    # Descending chronological order
    dates = [f["filing_date"] for f in body["filings"]]
    assert dates == sorted(dates, reverse=True)
    f0 = body["filings"][0]
    assert {"accession_number", "filing_date", "filing_type"} <= set(f0)
    assert f0["filing_type"] in ("10-K", "10-K/A")


def test_tenk_history_404_for_unknown_symbol():
    client = TestClient(app)
    r = client.get("/instruments/XYZNOTREAL/filings/10-k/history")
    assert r.status_code == 404
```

- [ ] **Step 2: Run to verify failure**

Run: `uv run pytest tests/api/test_instruments_tenk_history_endpoint.py -v`
Expected: FAIL — route does not exist (404 on a real symbol, not just the unknown-symbol case).

- [ ] **Step 3: Implement service helper**

Add to `app/services/business_summary.py`:

```python
@dataclass(frozen=True)
class TenKHistoryRow:
    accession_number: str
    filing_date: date
    filing_type: str


def list_10k_history(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
) -> tuple[TenKHistoryRow, ...]:
    """Return all 10-K + 10-K/A filings for ``instrument_id`` newest-
    first. Reads from ``filing_events`` (the canonical filings index)
    so we surface every filing, not just those parsed into
    ``instrument_business_summary_sections``."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT accession_number, filing_date, filing_type
            FROM filing_events
            WHERE instrument_id = %s
              AND filing_type IN ('10-K', '10-K/A')
            ORDER BY filing_date DESC, accession_number DESC
            """,
            (instrument_id,),
        )
        return tuple(
            TenKHistoryRow(
                accession_number=str(r[0]),
                filing_date=r[1],
                filing_type=str(r[2]),
            )
            for r in cur.fetchall()
        )
```

(If `from datetime import date` isn't already imported, add it to the imports at the top of the file.)

- [ ] **Step 4: Implement endpoint**

Add to `app/api/instruments.py` (near the existing `business_sections` route):

```python
class TenKHistoryFilingModel(BaseModel):
    accession_number: str
    filing_date: date
    filing_type: str


class TenKHistoryResponse(BaseModel):
    symbol: str
    filings: list[TenKHistoryFilingModel]


@router.get(
    "/{symbol}/filings/10-k/history",
    response_model=TenKHistoryResponse,
)
def get_instrument_tenk_history(
    symbol: str,
    conn: psycopg.Connection[object] = Depends(get_conn),
) -> TenKHistoryResponse:
    """Return all 10-K + 10-K/A filings for an instrument, descending."""
    from app.services.business_summary import list_10k_history

    symbol_clean = symbol.strip().upper()
    if not symbol_clean:
        raise HTTPException(status_code=400, detail="symbol is required")

    with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        cur.execute(
            """
            SELECT instrument_id, symbol FROM instruments
            WHERE UPPER(symbol) = %(s)s
            ORDER BY is_primary_listing DESC, instrument_id ASC
            LIMIT 1
            """,
            {"s": symbol_clean},
        )
        inst_row = cur.fetchone()

    if inst_row is None:
        raise HTTPException(status_code=404, detail=f"Instrument {symbol} not found")

    filings = list_10k_history(conn, instrument_id=int(inst_row["instrument_id"]))
    return TenKHistoryResponse(
        symbol=str(inst_row["symbol"]),
        filings=[
            TenKHistoryFilingModel(
                accession_number=f.accession_number,
                filing_date=f.filing_date,
                filing_type=f.filing_type,
            )
            for f in filings
        ],
    )
```

- [ ] **Step 5: Run to verify pass**

Run: `uv run pytest tests/api/test_instruments_tenk_history_endpoint.py -v`
Expected: PASS.

- [ ] **Step 6: Commit**

```bash
git add app/api/instruments.py app/services/business_summary.py tests/api/test_instruments_tenk_history_endpoint.py
git commit -m "feat(#559-phase2): /filings/10-k/history endpoint"
```

## Task 2.4: Frontend API client wiring

**Files:**
- Modify: `frontend/src/api/instruments.ts:209-215` (`fetchBusinessSections`)
- Modify: `frontend/src/api/instruments.ts` (add `fetchTenKHistory`)

- [ ] **Step 1: Update `fetchBusinessSections` to accept optional accession**

Replace the current `fetchBusinessSections`:

```ts
export function fetchBusinessSections(
  symbol: string,
  accession?: string,
): Promise<BusinessSectionsResponse> {
  const qs = accession !== undefined
    ? `?accession=${encodeURIComponent(accession)}`
    : "";
  return apiFetch<BusinessSectionsResponse>(
    `/instruments/${encodeURIComponent(symbol)}/business_sections${qs}`,
  );
}
```

Extend `BusinessSection` to include `tables`:

```ts
export interface BusinessTable {
  order: number;
  headers: string[];
  rows: string[][];
}

export interface BusinessSection {
  section_order: number;
  section_key: string;
  section_label: string;
  body: string;
  cross_references: BusinessCrossReference[];
  tables: BusinessTable[];
}
```

- [ ] **Step 2: Add `fetchTenKHistory`**

```ts
export interface TenKHistoryFiling {
  accession_number: string;
  filing_date: string; // ISO yyyy-mm-dd
  filing_type: string; // "10-K" | "10-K/A"
}

export interface TenKHistoryResponse {
  symbol: string;
  filings: TenKHistoryFiling[];
}

export function fetchTenKHistory(symbol: string): Promise<TenKHistoryResponse> {
  return apiFetch<TenKHistoryResponse>(
    `/instruments/${encodeURIComponent(symbol)}/filings/10-k/history`,
  );
}
```

- [ ] **Step 3: Typecheck**

Run: `pnpm --dir frontend typecheck`
Expected: 0 errors.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/api/instruments.ts
git commit -m "feat(#559-phase2): frontend API client for accession + 10-K history"
```

## Task 2.5: `EmbeddedTable` component

**Files:**
- Create: `frontend/src/components/instrument/EmbeddedTable.tsx`

- [ ] **Step 1: Create the component**

```tsx
/**
 * EmbeddedTable — renders one ParsedTable from a 10-K Item 1 section
 * body. Headers row + data rows, monospaced numeric columns, narrow
 * left rail spacing so it sits naturally inside reading prose (#559).
 */

import type { BusinessTable } from "@/api/instruments";

export interface EmbeddedTableProps {
  readonly table: BusinessTable;
}

export function EmbeddedTable({ table }: EmbeddedTableProps): JSX.Element {
  return (
    <table className="my-4 w-full border-collapse text-sm">
      <thead>
        <tr className="border-b border-slate-300 bg-slate-50 text-left text-xs uppercase tracking-wider text-slate-600">
          {table.headers.map((h, i) => (
            <th key={i} className="px-3 py-2 font-medium">
              {h}
            </th>
          ))}
        </tr>
      </thead>
      <tbody>
        {table.rows.map((row, rIdx) => (
          <tr
            key={rIdx}
            className="border-b border-slate-100 last:border-0"
          >
            {row.map((cell, cIdx) => (
              <td
                key={cIdx}
                className={`px-3 py-1.5 ${cIdx === 0 ? "" : "tabular-nums text-right"}`}
              >
                {cell}
              </td>
            ))}
          </tr>
        ))}
      </tbody>
    </table>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/instrument/EmbeddedTable.tsx
git commit -m "feat(#559-phase2): EmbeddedTable component"
```

## Task 2.6: `CrossRefPopover` component

**Files:**
- Create: `frontend/src/components/instrument/CrossRefPopover.tsx`

- [ ] **Step 1: Create the component**

```tsx
/**
 * CrossRefPopover — small popover triggered by clicking a cross-ref
 * chip in a 10-K section. Shows a 240-char excerpt of the targeted
 * section + an "Open full" link. For unresolvable targets (Note 5
 * when notes ingestion isn't on, Exhibit 21 — out of doc) it shows
 * a "Source: SEC iXBRL viewer" link instead (#559).
 */

import { useState } from "react";
import type { BusinessCrossReference, BusinessSection } from "@/api/instruments";

const PREVIEW_LEN = 240;

export interface CrossRefPopoverProps {
  readonly cref: BusinessCrossReference;
  /** All sections in the current 10-K — used to resolve "Item 1A" etc. */
  readonly sections: ReadonlyArray<BusinessSection>;
  /** SEC iXBRL viewer URL for fall-back when target isn't ingested. */
  readonly secViewerUrl: string | null;
}

function findTargetSection(
  cref: BusinessCrossReference,
  sections: ReadonlyArray<BusinessSection>,
): BusinessSection | null {
  if (cref.reference_type !== "item") return null;
  // cref.target like "Item 1A" — match on section_label prefix.
  const wanted = cref.target.toLowerCase().replace(/\s+/g, " ").trim();
  return (
    sections.find((s) =>
      s.section_label.toLowerCase().includes(wanted),
    ) ?? null
  );
}

function shortenBody(body: string): string {
  const flat = body.replace(/\s+/g, " ").trim();
  if (flat.length <= PREVIEW_LEN) return flat;
  const slice = flat.slice(0, PREVIEW_LEN);
  const cut = slice.lastIndexOf(" ");
  return (cut > PREVIEW_LEN * 0.7 ? slice.slice(0, cut) : slice) + "…";
}

export function CrossRefPopover({
  cref,
  sections,
  secViewerUrl,
}: CrossRefPopoverProps): JSX.Element {
  const [open, setOpen] = useState(false);
  const target = findTargetSection(cref, sections);

  return (
    <span className="relative inline-block">
      <button
        type="button"
        className="rounded bg-sky-100 px-1.5 py-0.5 text-[11px] font-medium text-sky-700 hover:bg-sky-200"
        onClick={() => setOpen((v) => !v)}
        aria-expanded={open}
      >
        {cref.target}
      </button>
      {open && (
        <span className="absolute left-0 top-full z-20 mt-1 block w-72 rounded border border-slate-200 bg-white p-3 text-xs shadow-lg">
          <span className="block text-[10px] uppercase tracking-wider text-slate-500">
            {cref.reference_type === "item" ? "Preview" : "Reference"} · {cref.target}
          </span>
          {target ? (
            <>
              <span className="mt-1 block font-medium text-slate-800">
                {target.section_label}
              </span>
              <span className="mt-1 block leading-relaxed text-slate-700">
                {shortenBody(target.body)}
              </span>
              <a
                href={`#s-${target.section_order}-${target.section_key}`}
                className="mt-2 block text-sky-700 hover:underline"
                onClick={() => setOpen(false)}
              >
                Open full ↗
              </a>
            </>
          ) : (
            <>
              <span className="mt-1 block leading-relaxed text-slate-600">
                Not yet ingested in eBull. View the source on SEC.
              </span>
              {secViewerUrl !== null && (
                <a
                  href={secViewerUrl}
                  target="_blank"
                  rel="noopener noreferrer"
                  className="mt-2 block text-sky-700 hover:underline"
                >
                  Open on SEC iXBRL viewer ↗
                </a>
              )}
            </>
          )}
        </span>
      )}
    </span>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/instrument/CrossRefPopover.tsx
git commit -m "feat(#559-phase2): CrossRefPopover component"
```

## Task 2.7: `TenKMetadataRail` component

**Files:**
- Create: `frontend/src/components/instrument/TenKMetadataRail.tsx`

- [ ] **Step 1: Create the component**

```tsx
/**
 * TenKMetadataRail — right rail on the 10-K drilldown page. Shows the
 * current filing accession + a list of prior 10-Ks for cross-year
 * thesis comparison + the cross-ref items list (#559).
 */

import type { TenKHistoryFiling } from "@/api/instruments";
import { Link } from "react-router-dom";

export interface TenKMetadataRailProps {
  readonly symbol: string;
  readonly currentAccession: string | null;
  readonly history: ReadonlyArray<TenKHistoryFiling>;
  readonly relatedItems: ReadonlyArray<string>; // e.g. ["Item 1A", "Item 7"]
}

export function TenKMetadataRail({
  symbol,
  currentAccession,
  history,
  relatedItems,
}: TenKMetadataRailProps): JSX.Element {
  return (
    <aside className="space-y-4 text-xs">
      <section>
        <h3 className="mb-1 text-[10px] uppercase tracking-wider text-slate-500">
          Filing
        </h3>
        {currentAccession !== null ? (
          <p className="font-mono text-[11px] text-slate-700 break-all">
            {currentAccession}
          </p>
        ) : (
          <p className="text-slate-500">—</p>
        )}
      </section>

      {history.length > 0 && (
        <section>
          <h3 className="mb-1 text-[10px] uppercase tracking-wider text-slate-500">
            Prior 10-Ks
          </h3>
          <ul className="space-y-0.5">
            {history.map((f) => {
              const isCurrent = f.accession_number === currentAccession;
              return (
                <li key={f.accession_number}>
                  <Link
                    to={`/instrument/${encodeURIComponent(symbol)}/filings/10-k?accession=${encodeURIComponent(f.accession_number)}`}
                    className={`block hover:underline ${
                      isCurrent
                        ? "font-medium text-slate-900"
                        : "text-sky-700"
                    }`}
                  >
                    {f.filing_date.slice(0, 4)}
                    {f.filing_type === "10-K/A" ? " (amended)" : ""}
                    {isCurrent ? " · current" : ""}
                  </Link>
                </li>
              );
            })}
          </ul>
        </section>
      )}

      {relatedItems.length > 0 && (
        <section>
          <h3 className="mb-1 text-[10px] uppercase tracking-wider text-slate-500">
            Related items
          </h3>
          <ul className="space-y-0.5">
            {relatedItems.map((item) => (
              <li key={item}>
                <a href={`#ref-${item}`} className="text-sky-700 hover:underline">
                  {item} ↗
                </a>
              </li>
            ))}
          </ul>
        </section>
      )}
    </aside>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/instrument/TenKMetadataRail.tsx
git commit -m "feat(#559-phase2): TenKMetadataRail component"
```

## Task 2.8: Rewrite `Tenk10KDrilldownPage`

**Files:**
- Rewrite: `frontend/src/pages/Tenk10KDrilldownPage.tsx`

- [ ] **Step 1: Rewrite the page**

Replace the entire file content with:

```tsx
/**
 * /instrument/:symbol/filings/10-k[?accession=...] — full SEC 10-K
 * Item 1 drilldown (#559).
 *
 * Three-pane layout:
 *   - Left rail (180 px): TOC built from section_order + label
 *   - Center reader: full-width body with continuous vertical
 *     left rail (CSS ::before, not section borders, so multi-block
 *     children don't break the line)
 *   - Right rail (200 px): filing accession, prior 10-Ks list,
 *     cross-related items
 *
 * The body renders prose with embedded <table> blocks at sentinel
 * positions and cross-ref chips that pop a 240-char preview popover.
 *
 * `?accession=` deep-links to a specific historical 10-K. Default
 * (no query string) renders the latest filing.
 */

import {
  fetchBusinessSections,
  fetchTenKHistory,
  type BusinessCrossReference,
  type BusinessSection,
  type BusinessSectionsResponse,
  type TenKHistoryResponse,
} from "@/api/instruments";
import { CrossRefPopover } from "@/components/instrument/CrossRefPopover";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmbeddedTable } from "@/components/instrument/EmbeddedTable";
import { EmptyState } from "@/components/states/EmptyState";
import { TenKMetadataRail } from "@/components/instrument/TenKMetadataRail";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

// Sentinel kept in sync with app/services/business_summary.py
const TABLE_SENTINEL_RE = /␞TABLE_(\d+)␞/g;

function sectionAnchorId(s: BusinessSection): string {
  return `s-${s.section_order}-${s.section_key}`;
}

interface BodyPart {
  type: "prose" | "table";
  prose?: string;
  tableOrder?: number;
}

function splitBodyByTables(body: string): BodyPart[] {
  const parts: BodyPart[] = [];
  let cursor = 0;
  for (const m of body.matchAll(TABLE_SENTINEL_RE)) {
    const before = body.slice(cursor, m.index);
    if (before.trim().length > 0) parts.push({ type: "prose", prose: before });
    parts.push({ type: "table", tableOrder: Number(m[1]) });
    cursor = (m.index ?? 0) + m[0].length;
  }
  const tail = body.slice(cursor);
  if (tail.trim().length > 0) parts.push({ type: "prose", prose: tail });
  return parts;
}

function renderProseWithCrossRefs(
  prose: string,
  crefs: ReadonlyArray<BusinessCrossReference>,
  sections: ReadonlyArray<BusinessSection>,
  secViewerUrl: string | null,
): JSX.Element {
  // Build a single regex matching every cref.target, longest-first to
  // avoid "Item 1" eating "Item 1A".
  const targets = [...new Set(crefs.map((c) => c.target))].sort(
    (a, b) => b.length - a.length,
  );
  if (targets.length === 0) {
    return <p className="whitespace-pre-wrap leading-relaxed text-slate-700">{prose}</p>;
  }
  const escaped = targets.map((t) => t.replace(/[.*+?^${}()|[\]\\]/g, "\\$&"));
  const re = new RegExp(`\\b(${escaped.join("|")})\\b`, "g");
  const parts: (string | JSX.Element)[] = [];
  let cursor = 0;
  let key = 0;
  for (const m of prose.matchAll(re)) {
    const idx = m.index ?? 0;
    if (idx > cursor) parts.push(prose.slice(cursor, idx));
    const cref = crefs.find((c) => c.target === m[1]);
    if (cref !== undefined) {
      parts.push(
        <CrossRefPopover
          key={`cref-${key++}`}
          cref={cref}
          sections={sections}
          secViewerUrl={secViewerUrl}
        />,
      );
    } else {
      parts.push(m[0]);
    }
    cursor = idx + m[0].length;
  }
  if (cursor < prose.length) parts.push(prose.slice(cursor));
  return <p className="whitespace-pre-wrap leading-relaxed text-slate-700">{parts}</p>;
}

function SectionBody({
  section,
  allSections,
  secViewerUrl,
}: {
  readonly section: BusinessSection;
  readonly allSections: ReadonlyArray<BusinessSection>;
  readonly secViewerUrl: string | null;
}) {
  const parts = splitBodyByTables(section.body);
  return (
    <article
      id={sectionAnchorId(section)}
      className="relative pl-6 before:absolute before:bottom-0 before:left-0 before:top-0 before:w-0.5 before:bg-slate-200"
    >
      <h3 className="text-base font-semibold text-slate-900">{section.section_label}</h3>
      <div className="mt-2 space-y-3 text-sm">
        {parts.map((p, i) => {
          if (p.type === "prose" && p.prose !== undefined) {
            return (
              <div key={i}>
                {renderProseWithCrossRefs(
                  p.prose,
                  section.cross_references,
                  allSections,
                  secViewerUrl,
                )}
              </div>
            );
          }
          if (p.type === "table" && p.tableOrder !== undefined) {
            const t = section.tables[p.tableOrder];
            if (t === undefined) return null;
            return <EmbeddedTable key={i} table={t} />;
          }
          return null;
        })}
      </div>
    </article>
  );
}

function TOCRail({ sections }: { readonly sections: ReadonlyArray<BusinessSection> }) {
  return (
    <nav className="sticky top-4 max-h-[calc(100vh-2rem)] overflow-y-auto text-xs">
      <div className="mb-2 text-[10px] font-medium uppercase tracking-wider text-slate-500">
        Sections
      </div>
      <ul className="space-y-1">
        {sections.map((s) => (
          <li key={sectionAnchorId(s)}>
            <a
              href={`#${sectionAnchorId(s)}`}
              className="block truncate text-slate-700 hover:text-sky-700 hover:underline"
              title={s.section_label}
            >
              {s.section_label}
            </a>
          </li>
        ))}
      </ul>
    </nav>
  );
}

function secViewerUrlFor(accession: string | null): string | null {
  if (accession === null) return null;
  // SEC's iXBRL viewer pattern; accession numbers in their URL form
  // omit the dashes.
  const naked = accession.replace(/-/g, "");
  return `https://www.sec.gov/cgi-bin/viewer?action=view&cik=&accession_number=${naked}`;
}

function Body({
  data,
  history,
  symbol,
}: {
  readonly data: BusinessSectionsResponse;
  readonly history: TenKHistoryResponse;
  readonly symbol: string;
}) {
  if (data.sections.length === 0) {
    return (
      <EmptyState
        title="No 10-K Item 1 on file"
        description="No 10-K business description has been parsed for this instrument yet."
      />
    );
  }
  const allCrefs = data.sections.flatMap((s) => s.cross_references);
  const relatedItems = [
    ...new Set(
      allCrefs
        .filter((c) => c.reference_type === "item")
        .map((c) => c.target),
    ),
  ];
  const secViewer = secViewerUrlFor(data.source_accession);

  return (
    <div className="grid gap-6 grid-cols-[180px_minmax(0,1fr)_200px]">
      <aside className="hidden lg:block">
        <TOCRail sections={data.sections} />
      </aside>
      <div className="min-w-0 space-y-6">
        <header className="border-b border-slate-200 pb-3">
          <Link
            to={`/instrument/${encodeURIComponent(symbol)}`}
            className="text-xs text-sky-700 hover:underline"
          >
            ← Back to {symbol}
          </Link>
          <h2 className="mt-1 text-lg font-semibold text-slate-900">
            Form 10-K · Item 1 Business
          </h2>
        </header>
        {data.sections.map((s) => (
          <SectionBody
            key={sectionAnchorId(s)}
            section={s}
            allSections={data.sections}
            secViewerUrl={secViewer}
          />
        ))}
      </div>
      <div className="hidden lg:block">
        <TenKMetadataRail
          symbol={symbol}
          currentAccession={data.source_accession}
          history={history.filings}
          relatedItems={relatedItems}
        />
      </div>
    </div>
  );
}

export function Tenk10KDrilldownPage() {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams] = useSearchParams();
  const accession = searchParams.get("accession") ?? undefined;

  const sectionsState = useAsync<BusinessSectionsResponse>(
    useCallback(() => fetchBusinessSections(symbol, accession), [symbol, accession]),
    [symbol, accession],
  );
  const historyState = useAsync<TenKHistoryResponse>(
    useCallback(() => fetchTenKHistory(symbol), [symbol]),
    [symbol],
  );

  return (
    <div className="mx-auto max-w-screen-2xl p-4">
      <Section title={`${symbol} — 10-K narrative`}>
        {sectionsState.loading || historyState.loading ? (
          <SectionSkeleton rows={6} />
        ) : sectionsState.error !== null ? (
          <SectionError onRetry={sectionsState.refetch} />
        ) : sectionsState.data === null ? (
          <EmptyState
            title="Business narrative unavailable"
            description="Could not load 10-K Item 1 sections for this instrument."
          />
        ) : (
          <Body
            data={sectionsState.data}
            history={historyState.data ?? { symbol, filings: [] }}
            symbol={symbol}
          />
        )}
      </Section>
    </div>
  );
}
```

- [ ] **Step 2: Run typecheck**

Run: `pnpm --dir frontend typecheck`
Expected: 0 errors.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/pages/Tenk10KDrilldownPage.tsx
git commit -m "feat(#559-phase2): three-pane 10-K drilldown with tables + popovers"
```

## Task 2.9: Vitest test for the new page

**Files:**
- Create: `frontend/src/pages/Tenk10KDrilldownPage.test.tsx`

- [ ] **Step 1: Write the test**

```tsx
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter, Routes, Route } from "react-router-dom";
import { Tenk10KDrilldownPage } from "@/pages/Tenk10KDrilldownPage";
import * as api from "@/api/instruments";

describe("Tenk10KDrilldownPage", () => {
  it("renders three panes: TOC, body with embedded table, metadata rail", async () => {
    vi.spyOn(api, "fetchBusinessSections").mockResolvedValue({
      symbol: "GME",
      source_accession: "0001326380-26-000001",
      sections: [
        {
          section_order: 0,
          section_key: "general",
          section_label: "General",
          body: "We sell games. ␞TABLE_0␞ Stores worldwide.",
          cross_references: [],
          tables: [
            {
              order: 0,
              headers: ["Segment", "Stores"],
              rows: [
                ["United States", "1,598"],
                ["Europe", "308"],
              ],
            },
          ],
        },
      ],
    });
    vi.spyOn(api, "fetchTenKHistory").mockResolvedValue({
      symbol: "GME",
      filings: [
        {
          accession_number: "0001326380-26-000001",
          filing_date: "2026-03-24",
          filing_type: "10-K",
        },
        {
          accession_number: "0001326380-25-000001",
          filing_date: "2025-03-24",
          filing_type: "10-K",
        },
      ],
    });

    render(
      <MemoryRouter initialEntries={["/instrument/GME/filings/10-k"]}>
        <Routes>
          <Route
            path="/instrument/:symbol/filings/10-k"
            element={<Tenk10KDrilldownPage />}
          />
        </Routes>
      </MemoryRouter>,
    );

    expect(await screen.findByText("General")).toBeInTheDocument();
    expect(screen.getByText("United States")).toBeInTheDocument();
    expect(screen.getByText("1,598")).toBeInTheDocument();
    expect(screen.getByText("2025")).toBeInTheDocument(); // prior 10-K rail entry
    // Sentinel string must not leak to the visible text
    expect(screen.queryByText(/TABLE_0/)).toBeNull();
  });
});
```

- [ ] **Step 2: Run the test**

Run: `pnpm --dir frontend test -- Tenk10KDrilldownPage`
Expected: PASS.

- [ ] **Step 3: Run pre-push gate**

Run:
```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest && pnpm --dir frontend typecheck && pnpm --dir frontend test:unit
```
Expected: all green.

- [ ] **Step 4: Codex review**

Run: `codex.cmd exec review`
Expected: clean or actionable findings.

- [ ] **Step 5: Push + open PR**

```bash
git add frontend/src/pages/Tenk10KDrilldownPage.test.tsx
git commit -m "test(#559-phase2): Tenk10KDrilldownPage three-pane render"
git push -u origin feature/559-phase2-tenk-drilldown
gh pr create --title "feat(#559-phase2): three-pane 10-K drilldown" --body "$(cat <<'EOF'
## What

- Backend: `?accession=` query param on `/business_sections`, new `/filings/10-k/history`.
- Frontend: full-width three-pane layout (TOC | reader | metadata rail), embedded table renderer, cross-ref hover popover, prior-10-Ks list.
- Continuous vertical line via CSS `::before` (no more gaps from block-child margins).

## Why

Phase 2 of the density-grid spec. Uses the `tables_json` payload from Phase 1.

## Test plan

- [ ] Backend: accession + history endpoint contracts.
- [ ] Frontend: Vitest for three-pane render + table + history rail.
- [ ] Manual: load `/instrument/GME/filings/10-k` and confirm full-bleed width, continuous left rail, embedded segment table, popover on "Item 1A" chip.
EOF
)"
```

- [ ] **Step 6: Poll review + CI; resolve every comment; merge on APPROVE + green**

---

# Phase 3 — instrument page density grid

**Branch:** `feature/559-phase3-density-grid`

**Goal:** Replace the stacked Research-tab content with a chart-led density grid (chart 2×2 + 6 panes). New 5-row Filings pane links to the drilldowns from Phase 2 (and Phase 4's 8-K page).

## Task 3.1: `FilingsPane` component

**Files:**
- Create: `frontend/src/components/instrument/FilingsPane.tsx`

- [ ] **Step 1: Create the component**

```tsx
/**
 * FilingsPane — 5-row recent-filings list (8-K + 10-K) on the
 * instrument page density grid (#559). Each row links to the
 * corresponding drilldown route. Read-only — the canonical filings
 * tab still lives in the page tabs nav.
 */

import { fetchFilings } from "@/api/filings";
import type { FilingsListResponse } from "@/api/types";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback } from "react";
import { Link } from "react-router-dom";

const ROW_LIMIT = 5;

const TYPES_WITH_DRILLDOWN = new Set(["8-K", "8-K/A", "10-K", "10-K/A"]);

function drilldownLink(symbol: string, filingType: string | null): string | null {
  if (filingType === null || !TYPES_WITH_DRILLDOWN.has(filingType)) return null;
  const symbolEnc = encodeURIComponent(symbol);
  if (filingType.startsWith("10-K")) {
    // 10-K drilldown defaults to the latest filing — no accession
    // needed from the row. Operator picks an older year via the
    // metadata rail's prior-10-Ks list once on the drilldown page.
    return `/instrument/${symbolEnc}/filings/10-k`;
  }
  // 8-K family — list page shows all filings; row click on the list
  // page itself handles per-accession selection.
  return `/instrument/${symbolEnc}/filings/8-k`;
}

export interface FilingsPaneProps {
  readonly instrumentId: number;
  readonly symbol: string;
}

export function FilingsPane({ instrumentId, symbol }: FilingsPaneProps): JSX.Element {
  const state = useAsync<FilingsListResponse>(
    useCallback(() => fetchFilings(instrumentId, 0, ROW_LIMIT), [instrumentId]),
    [instrumentId],
  );

  return (
    <Section title="Recent filings">
      {state.loading ? (
        <SectionSkeleton rows={5} />
      ) : state.error !== null ? (
        <SectionError onRetry={state.refetch} />
      ) : state.data === null || state.data.items.length === 0 ? (
        <EmptyState
          title="No filings"
          description="Filings appear once SEC EDGAR has been crawled for this instrument."
        />
      ) : (
        <ul className="space-y-1.5 text-xs">
          {state.data.items.slice(0, ROW_LIMIT).map((f) => {
            const link = drilldownLink(symbol, f.filing_type ?? null);
            const label = (
              <span className="flex items-baseline gap-2">
                <span className="text-slate-500">{f.filing_date}</span>
                <span className="rounded bg-slate-100 px-1 py-0.5 text-[10px] text-slate-600">
                  {f.filing_type ?? "?"}
                </span>
                <span className="truncate text-slate-700">
                  {f.extracted_summary ?? f.filing_type ?? "filing"}
                </span>
              </span>
            );
            return (
              <li key={f.filing_event_id}>
                {link !== null ? (
                  <Link to={link} className="hover:underline">
                    {label}
                  </Link>
                ) : (
                  label
                )}
              </li>
            );
          })}
        </ul>
      )}
    </Section>
  );
}
```

- [ ] **Step 2: Vitest**

Create `frontend/src/components/instrument/FilingsPane.test.tsx`:

```tsx
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import * as filingsApi from "@/api/filings";

describe("FilingsPane", () => {
  it("renders 5 rows max with drilldown links for 8-K + 10-K", async () => {
    vi.spyOn(filingsApi, "fetchFilings").mockResolvedValue({
      total: 8,
      items: Array.from({ length: 8 }, (_, i) => ({
        filing_event_id: i + 1,
        instrument_id: 1,
        filing_date: `2026-03-${(i + 1).toString().padStart(2, "0")}`,
        filing_type: i % 2 === 0 ? "10-K" : "8-K",
        provider: "sec_edgar",
        red_flag_score: null,
        extracted_summary: `summary ${i}`,
        primary_document_url: null,
        source_url: null,
        created_at: "2026-03-01T00:00:00Z",
      })) as never,
    } as never);
    render(
      <MemoryRouter>
        <FilingsPane instrumentId={1} symbol="GME" />
      </MemoryRouter>,
    );
    const rows = await screen.findAllByText(/summary \d/);
    expect(rows.length).toBeLessThanOrEqual(5);
  });
});
```

- [ ] **Step 3: Run + commit**

```bash
git checkout -b feature/559-phase3-density-grid
pnpm --dir frontend typecheck
pnpm --dir frontend test -- FilingsPane
git add frontend/src/components/instrument/FilingsPane.tsx frontend/src/components/instrument/FilingsPane.test.tsx
git commit -m "feat(#559-phase3): FilingsPane component"
```

## Task 3.2: `DensityGrid` component

**Files:**
- Create: `frontend/src/components/instrument/DensityGrid.tsx`

- [ ] **Step 1: Create the component**

```tsx
/**
 * DensityGrid — Bloomberg-style 3-column grid for the instrument
 * Research tab (#559). Chart occupies a 2x2 cell top-left; right
 * column stacks key-stats / thesis / SEC profile / filings; bottom
 * rows hold segments / dividends-insider / news.
 *
 * Responsive: at viewport widths below `lg` the grid degrades to
 * a single column. Pane order reflects priority: chart → key-stats
 * → thesis → filings → SEC-profile → segments → dividends-insider
 * → news. Each pane scrolls internally rather than pushing the
 * page taller.
 */

import { BusinessSectionsTeaser } from "@/components/instrument/BusinessSectionsTeaser";
import { DividendsPanel } from "@/components/instrument/DividendsPanel";
import { FilingsPane } from "@/components/instrument/FilingsPane";
import { InsiderActivityPanel } from "@/components/instrument/InsiderActivityPanel";
import { PriceChart } from "@/components/instrument/PriceChart";
import { SecProfilePanel } from "@/components/instrument/SecProfilePanel";
import { Section } from "@/components/dashboard/Section";
import type { JSX } from "react";

import type { CapabilityCell, InstrumentSummary, ThesisDetail } from "@/api/types";
import { activeProviders } from "@/lib/capabilityProviders";

export interface DensityGridProps {
  readonly summary: InstrumentSummary;
  readonly thesis: ThesisDetail | null;
  readonly thesisErrored: boolean;
  readonly keyStatsBlock: JSX.Element;
  readonly thesisBlock: JSX.Element;
  readonly newsBlock: JSX.Element;
}

const EMPTY_CELL: CapabilityCell = { providers: [], data_present: {} };

export function DensityGrid({
  summary,
  keyStatsBlock,
  thesisBlock,
  newsBlock,
}: DensityGridProps): JSX.Element {
  const symbol = summary.identity.symbol;
  const hasSec = summary.has_sec_cik;
  const dividends = summary.capabilities.dividends ?? EMPTY_CELL;
  const insider = summary.capabilities.insider ?? EMPTY_CELL;
  const dividendProviders = activeProviders(dividends);
  const insiderProviders = activeProviders(insider);

  return (
    <div className="grid grid-cols-1 gap-3 lg:grid-cols-[2fr_1fr_1fr] lg:auto-rows-[220px]">
      {/* Chart pane: 2 cols × 2 rows top-left */}
      <div className="lg:col-start-1 lg:col-end-2 lg:row-start-1 lg:row-end-3 overflow-hidden rounded-md border border-slate-200 bg-white p-3 shadow-sm">
        <PriceChart symbol={symbol} />
      </div>

      {/* Right column row 1 */}
      <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">
        {keyStatsBlock}
      </div>
      <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">
        {thesisBlock}
      </div>

      {/* Right column row 2 */}
      <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">
        {hasSec ? <SecProfilePanel symbol={symbol} /> : (
          <Section title="SEC profile">
            <p className="text-xs text-slate-500">No SEC coverage</p>
          </Section>
        )}
      </div>
      <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">
        <FilingsPane instrumentId={summary.instrument_id} symbol={symbol} />
      </div>

      {/* Bottom row: spans full width */}
      <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm lg:col-span-2">
        {hasSec ? (
          <BusinessSectionsTeaser symbol={symbol} />
        ) : (
          <Section title="Company narrative">
            <p className="text-xs text-slate-500">No 10-K coverage</p>
          </Section>
        )}
      </div>
      <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm">
        {newsBlock}
      </div>

      {/* Dividends + insider combined card */}
      {(dividendProviders.length > 0 || insiderProviders.length > 0) && (
        <div className="overflow-auto rounded-md border border-slate-200 bg-white p-3 shadow-sm lg:col-span-3">
          <div className="grid gap-3 md:grid-cols-2">
            {dividendProviders.map((p) => (
              <DividendsPanel key={`div-${p}`} symbol={symbol} provider={p} />
            ))}
            {insiderProviders.map((p) => (
              <InsiderActivityPanel key={`ins-${p}`} symbol={symbol} provider={p} />
            ))}
          </div>
        </div>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Vitest**

Create `frontend/src/components/instrument/DensityGrid.test.tsx`:

```tsx
import { describe, expect, it } from "vitest";
import { render, screen } from "@testing-library/react";
import { MemoryRouter } from "react-router-dom";
import { DensityGrid } from "@/components/instrument/DensityGrid";

const summary = {
  instrument_id: 1,
  has_sec_cik: true,
  identity: {
    symbol: "GME",
    display_name: "GameStop",
    market_cap: "1000000",
    sector: null,
  },
  capabilities: {},
  key_stats: null,
} as never;

describe("DensityGrid", () => {
  it("renders the chart, the slot blocks, and FilingsPane title", () => {
    render(
      <MemoryRouter>
        <DensityGrid
          summary={summary}
          thesis={null}
          thesisErrored={false}
          keyStatsBlock={<div>KEY STATS BLOCK</div>}
          thesisBlock={<div>THESIS BLOCK</div>}
          newsBlock={<div>NEWS BLOCK</div>}
        />
      </MemoryRouter>,
    );
    expect(screen.getByText("KEY STATS BLOCK")).toBeInTheDocument();
    expect(screen.getByText("THESIS BLOCK")).toBeInTheDocument();
    expect(screen.getByText("NEWS BLOCK")).toBeInTheDocument();
    expect(screen.getByText("Recent filings")).toBeInTheDocument();
  });
});
```

- [ ] **Step 3: Commit**

```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test -- DensityGrid
git add frontend/src/components/instrument/DensityGrid.tsx frontend/src/components/instrument/DensityGrid.test.tsx
git commit -m "feat(#559-phase3): DensityGrid container"
```

## Task 3.3: Rewire `ResearchTab` to render the grid

**Files:**
- Modify: `frontend/src/components/instrument/ResearchTab.tsx`

- [ ] **Step 1: Replace the JSX of `ResearchTab` (keep the existing helper functions)**

Replace the `return (...)` block of `ResearchTab` and remove the unused capability-panel iteration (now wrapped inside `DensityGrid`):

```tsx
import { DensityGrid } from "@/components/instrument/DensityGrid";
// remove: BusinessSectionsTeaser / DividendsPanel / EightKEventsPanel / InsiderActivityPanel / SecProfilePanel imports

export function ResearchTab({
  summary,
  thesis,
  thesisErrored = false,
}: ResearchTabProps): JSX.Element {
  const stats = summary.key_stats;
  const fs = stats?.field_source ?? undefined;

  const keyStatsBlock = (
    <Section title="Key statistics">
      {stats === null ? (
        <EmptyState
          title="No key stats"
          description="No provider returned key stats for this ticker."
        />
      ) : (
        <dl className="grid grid-cols-[auto_1fr] gap-x-4 gap-y-2 text-sm">
          <KeyStat label="Market cap" value={formatMarketCap(summary.identity.market_cap)} />
          <KeyStat label="P/E ratio" value={formatDecimal(stats.pe_ratio)} source={fs?.pe_ratio} />
          <KeyStat label="P/B ratio" value={formatDecimal(stats.pb_ratio)} source={fs?.pb_ratio} />
          <KeyStat label="Dividend yield" value={formatDecimal(stats.dividend_yield, { percent: true })} source={fs?.dividend_yield} />
          <KeyStat label="Payout ratio" value={formatDecimal(stats.payout_ratio, { percent: true })} source={fs?.payout_ratio} />
          <KeyStat label="ROE" value={formatDecimal(stats.roe, { percent: true })} source={fs?.roe} />
          <KeyStat label="ROA" value={formatDecimal(stats.roa, { percent: true })} source={fs?.roa} />
          <KeyStat label="Debt / Equity" value={formatDecimal(stats.debt_to_equity)} source={fs?.debt_to_equity} />
          <KeyStat label="Revenue growth (YoY)" value={formatDecimal(stats.revenue_growth_yoy, { percent: true })} source={fs?.revenue_growth_yoy} />
          <KeyStat label="Earnings growth (YoY)" value={formatDecimal(stats.earnings_growth_yoy, { percent: true })} source={fs?.earnings_growth_yoy} />
        </dl>
      )}
    </Section>
  );

  const thesisBlock = (
    <Section title="Thesis">
      <ThesisPanel thesis={thesis} errored={thesisErrored} />
    </Section>
  );

  const newsBlock = (
    <Section title="Recent news">
      <p className="text-xs text-slate-500">News tab still has the full feed.</p>
    </Section>
  );

  return (
    <DensityGrid
      summary={summary}
      thesis={thesis}
      thesisErrored={thesisErrored}
      keyStatsBlock={keyStatsBlock}
      thesisBlock={thesisBlock}
      newsBlock={newsBlock}
    />
  );
}
```

- [ ] **Step 2: Remove now-unused chart-on-top from `InstrumentPage`**

In `frontend/src/pages/InstrumentPage.tsx:670-684`, the Research tab block currently renders `<PriceChart>` then `<ResearchTab>`. The chart now lives inside the grid — remove the standalone `<PriceChart>` so it doesn't double-render:

```tsx
{activeTab === "research" && (
  <ResearchTab
    summary={summary}
    thesis={thesisAsync.data}
    thesisErrored={thesisErrSticky}
  />
)}
```

(Drop the surrounding `<div className="space-y-4">` and the inline `<PriceChart>` card.)

- [ ] **Step 3: Run frontend gates**

Run:
```bash
pnpm --dir frontend typecheck
pnpm --dir frontend test:unit
```
Expected: 0 errors, 0 test failures. (Existing `ResearchTab` consumers may need the test fixture updated — adjust as needed.)

- [ ] **Step 4: Commit**

```bash
git add frontend/src/components/instrument/ResearchTab.tsx frontend/src/pages/InstrumentPage.tsx
git commit -m "feat(#559-phase3): ResearchTab renders DensityGrid; chart moves into grid"
```

## Task 3.4: Pre-push gate + Codex + push + merge

- [ ] **Step 1: Full gate**

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest && pnpm --dir frontend typecheck && pnpm --dir frontend test:unit
```

- [ ] **Step 2: Codex review**

Run: `codex.cmd exec review`

- [ ] **Step 3: Push + PR**

```bash
git push -u origin feature/559-phase3-density-grid
gh pr create --title "feat(#559-phase3): instrument page density grid" --body "$(cat <<'EOF'
## What

- `DensityGrid` 3-col chart-led layout (chart 2x2 + 6 panes).
- New `FilingsPane` (5-row recent filings, links to drilldowns).
- `ResearchTab` rewritten to render the grid; chart no longer renders twice.

## Why

Phase 3 of the density-grid spec — replaces stacked Research-tab content with Bloomberg-style information density.

## Test plan

- [ ] Vitest: DensityGrid + FilingsPane unit tests.
- [ ] Manual: load `/instrument/GME`, confirm chart + 6 panes visible without scrolling above `lg`, single column below `lg`.
EOF
)"
```

- [ ] **Step 4: Poll review + CI; resolve every comment; merge on APPROVE + green**

---

# Phase 4 — 8-K filterable detail page

**Branch:** `feature/559-phase4-eight-k-page`

**Goal:** Move 8-K rendering from the inline `EightKEventsPanel` into a dedicated `/instrument/:symbol/filings/8-k` route with a filter strip + detail panel.

## Task 4.1: `EightKDetailPanel` component

**Files:**
- Create: `frontend/src/components/instrument/EightKDetailPanel.tsx`

- [ ] **Step 1: Create the component**

```tsx
/**
 * EightKDetailPanel — right-side detail for the row currently
 * selected in the 8-K filterable list. Shows item bodies +
 * exhibits + signature block (#559).
 */

import type { EightKFiling } from "@/api/instruments";

export interface EightKDetailPanelProps {
  readonly filing: EightKFiling | null;
}

const SEVERITY_TONE: Record<string, string> = {
  high: "bg-red-100 text-red-700",
  medium: "bg-amber-100 text-amber-700",
  low: "bg-slate-100 text-slate-600",
};

export function EightKDetailPanel({ filing }: EightKDetailPanelProps): JSX.Element {
  if (filing === null) {
    return (
      <div className="rounded border border-slate-200 bg-white p-4 text-sm text-slate-500">
        Select a row to view item bodies + exhibits.
      </div>
    );
  }
  return (
    <div className="space-y-4 rounded border border-slate-200 bg-white p-4 text-sm">
      <div>
        <div className="text-[10px] uppercase tracking-wider text-slate-500">
          Filing
        </div>
        <div className="font-mono text-xs">{filing.accession_number}</div>
        <div className="text-xs text-slate-500">
          {filing.date_of_report} · {filing.reporting_party}
          {filing.is_amendment ? " · amendment" : ""}
        </div>
      </div>
      {filing.items.map((item) => (
        <section key={item.item_code}>
          <header className="flex items-baseline gap-2">
            <span
              className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${SEVERITY_TONE[item.severity] ?? SEVERITY_TONE.low}`}
            >
              Item {item.item_code}
            </span>
            <span className="text-xs font-medium text-slate-800">{item.item_label}</span>
          </header>
          <p className="mt-1 whitespace-pre-wrap leading-relaxed text-slate-700">
            {item.body}
          </p>
        </section>
      ))}
      {filing.exhibits.length > 0 && (
        <section>
          <div className="text-[10px] uppercase tracking-wider text-slate-500">
            Exhibits
          </div>
          <ul className="mt-1 space-y-0.5 text-xs">
            {filing.exhibits.map((e) => (
              <li key={e.exhibit_number}>
                · {e.exhibit_number}
                {e.description ? ` — ${e.description}` : ""}
              </li>
            ))}
          </ul>
        </section>
      )}
      {filing.primary_document_url !== null && (
        <a
          href={filing.primary_document_url}
          target="_blank"
          rel="noopener noreferrer"
          className="text-xs text-sky-700 hover:underline"
        >
          Open full filing on SEC ↗
        </a>
      )}
    </div>
  );
}
```

(Reuse the response shape exported by `frontend/src/api/instruments.ts::fetchEightKFilings` — re-import or alias if the type isn't named `EightKFiling` there. Look at the existing export and use the actual name.)

- [ ] **Step 2: Commit**

```bash
git checkout -b feature/559-phase4-eight-k-page
git add frontend/src/components/instrument/EightKDetailPanel.tsx
git commit -m "feat(#559-phase4): EightKDetailPanel component"
```

## Task 4.2: `EightKFilterStrip` component

**Files:**
- Create: `frontend/src/components/instrument/EightKFilterStrip.tsx`

- [ ] **Step 1: Create the component**

```tsx
/**
 * EightKFilterStrip — controls for the 8-K filterable list (#559).
 * Severity dropdown + free-text item-code filter + date range.
 * State held in URL query string by the parent so deep-links work.
 */

import type { JSX } from "react";

export interface EightKFilters {
  readonly severity: "" | "high" | "medium" | "low";
  readonly itemCode: string;
  readonly dateFrom: string; // ISO yyyy-mm-dd, "" = no bound
  readonly dateTo: string;
}

export interface EightKFilterStripProps {
  readonly value: EightKFilters;
  readonly onChange: (next: EightKFilters) => void;
}

export function EightKFilterStrip({ value, onChange }: EightKFilterStripProps): JSX.Element {
  return (
    <div className="flex flex-wrap items-end gap-3 rounded border border-slate-200 bg-slate-50 p-3 text-xs">
      <label className="flex flex-col">
        <span className="text-slate-500">Severity</span>
        <select
          className="mt-0.5 rounded border border-slate-300 bg-white px-2 py-1"
          value={value.severity}
          onChange={(e) =>
            onChange({ ...value, severity: e.target.value as EightKFilters["severity"] })
          }
        >
          <option value="">all</option>
          <option value="high">high</option>
          <option value="medium">medium</option>
          <option value="low">low</option>
        </select>
      </label>
      <label className="flex flex-col">
        <span className="text-slate-500">Item code</span>
        <input
          type="text"
          placeholder="e.g. 5.02"
          className="mt-0.5 rounded border border-slate-300 bg-white px-2 py-1"
          value={value.itemCode}
          onChange={(e) => onChange({ ...value, itemCode: e.target.value })}
        />
      </label>
      <label className="flex flex-col">
        <span className="text-slate-500">From</span>
        <input
          type="date"
          className="mt-0.5 rounded border border-slate-300 bg-white px-2 py-1"
          value={value.dateFrom}
          onChange={(e) => onChange({ ...value, dateFrom: e.target.value })}
        />
      </label>
      <label className="flex flex-col">
        <span className="text-slate-500">To</span>
        <input
          type="date"
          className="mt-0.5 rounded border border-slate-300 bg-white px-2 py-1"
          value={value.dateTo}
          onChange={(e) => onChange({ ...value, dateTo: e.target.value })}
        />
      </label>
      {(value.severity !== "" ||
        value.itemCode !== "" ||
        value.dateFrom !== "" ||
        value.dateTo !== "") && (
        <button
          type="button"
          className="ml-auto rounded border border-slate-300 px-2 py-1 hover:bg-white"
          onClick={() =>
            onChange({ severity: "", itemCode: "", dateFrom: "", dateTo: "" })
          }
        >
          Reset
        </button>
      )}
    </div>
  );
}
```

- [ ] **Step 2: Commit**

```bash
git add frontend/src/components/instrument/EightKFilterStrip.tsx
git commit -m "feat(#559-phase4): EightKFilterStrip component"
```

## Task 4.3: `EightKListPage`

**Files:**
- Create: `frontend/src/pages/EightKListPage.tsx`
- Modify: `frontend/src/App.tsx` (register route)

- [ ] **Step 1: Create the page**

```tsx
/**
 * /instrument/:symbol/filings/8-k — filterable 8-K list with detail
 * panel (#559).
 *
 * Filter state lives in URL query string (severity / itemCode /
 * dateFrom / dateTo / accession=) so deep-links work.
 */

import { fetchEightKFilings } from "@/api/instruments";
import type { EightKFiling, EightKFilingsResponse } from "@/api/instruments";
import { Section, SectionError, SectionSkeleton } from "@/components/dashboard/Section";
import { EightKDetailPanel } from "@/components/instrument/EightKDetailPanel";
import {
  EightKFilterStrip,
  type EightKFilters,
} from "@/components/instrument/EightKFilterStrip";
import { EmptyState } from "@/components/states/EmptyState";
import { useAsync } from "@/lib/useAsync";
import { useCallback, useMemo } from "react";
import { Link, useParams, useSearchParams } from "react-router-dom";

const SEVERITY_TONE: Record<string, string> = {
  high: "bg-red-100 text-red-700",
  medium: "bg-amber-100 text-amber-700",
  low: "bg-slate-100 text-slate-600",
};

function readFilters(p: URLSearchParams): EightKFilters {
  const severity = p.get("severity");
  return {
    severity:
      severity === "high" || severity === "medium" || severity === "low"
        ? severity
        : "",
    itemCode: p.get("itemCode") ?? "",
    dateFrom: p.get("dateFrom") ?? "",
    dateTo: p.get("dateTo") ?? "",
  };
}

function writeFilters(p: URLSearchParams, f: EightKFilters): URLSearchParams {
  const out = new URLSearchParams(p);
  if (f.severity === "") out.delete("severity");
  else out.set("severity", f.severity);
  if (f.itemCode === "") out.delete("itemCode");
  else out.set("itemCode", f.itemCode);
  if (f.dateFrom === "") out.delete("dateFrom");
  else out.set("dateFrom", f.dateFrom);
  if (f.dateTo === "") out.delete("dateTo");
  else out.set("dateTo", f.dateTo);
  return out;
}

function highestSeverity(filing: EightKFiling): string {
  for (const item of filing.items) {
    if (item.severity === "high") return "high";
  }
  for (const item of filing.items) {
    if (item.severity === "medium") return "medium";
  }
  return "low";
}

function applyFilters(
  filings: ReadonlyArray<EightKFiling>,
  f: EightKFilters,
): EightKFiling[] {
  return filings.filter((flg) => {
    if (f.severity !== "" && highestSeverity(flg) !== f.severity) return false;
    if (f.itemCode !== "") {
      if (!flg.items.some((i) => i.item_code.includes(f.itemCode))) return false;
    }
    if (f.dateFrom !== "" && flg.date_of_report < f.dateFrom) return false;
    if (f.dateTo !== "" && flg.date_of_report > f.dateTo) return false;
    return true;
  });
}

export function EightKListPage(): JSX.Element {
  const { symbol = "" } = useParams<{ symbol: string }>();
  const [searchParams, setSearchParams] = useSearchParams();
  const filters = readFilters(searchParams);
  const selectedAccession = searchParams.get("accession");

  const state = useAsync<EightKFilingsResponse>(
    useCallback(() => fetchEightKFilings(symbol, 100), [symbol]),
    [symbol],
  );

  const filtered = useMemo(
    () => (state.data === null ? [] : applyFilters(state.data.filings, filters)),
    [state.data, filters],
  );
  const selected =
    selectedAccession !== null
      ? (filtered.find((f) => f.accession_number === selectedAccession) ?? null)
      : null;

  function setFilters(next: EightKFilters): void {
    setSearchParams(writeFilters(searchParams, next), { replace: true });
  }
  function selectAccession(acc: string | null): void {
    const out = new URLSearchParams(searchParams);
    if (acc === null) out.delete("accession");
    else out.set("accession", acc);
    setSearchParams(out, { replace: true });
  }

  return (
    <div className="mx-auto max-w-screen-2xl space-y-3 p-4">
      <Section title={`${symbol} — 8-K filings`}>
        <Link
          to={`/instrument/${encodeURIComponent(symbol)}`}
          className="text-xs text-sky-700 hover:underline"
        >
          ← Back to {symbol}
        </Link>
        <div className="mt-3">
          <EightKFilterStrip value={filters} onChange={setFilters} />
        </div>
        {state.loading ? (
          <SectionSkeleton rows={5} />
        ) : state.error !== null ? (
          <SectionError onRetry={state.refetch} />
        ) : state.data === null || state.data.filings.length === 0 ? (
          <EmptyState
            title="No 8-K filings"
            description="No 8-K filings on file for this instrument."
          />
        ) : (
          <div className="mt-3 grid gap-4 lg:grid-cols-[3fr_2fr]">
            <div className="overflow-x-auto">
              <table className="min-w-full text-xs">
                <thead>
                  <tr className="border-b border-slate-200 text-left text-slate-500">
                    <th className="px-2 py-1">Date</th>
                    <th className="px-2 py-1">Items</th>
                    <th className="px-2 py-1">Severity</th>
                    <th className="px-2 py-1">Subject</th>
                  </tr>
                </thead>
                <tbody>
                  {filtered.map((f) => {
                    const isSelected = f.accession_number === selectedAccession;
                    const sev = highestSeverity(f);
                    return (
                      <tr
                        key={f.accession_number}
                        className={`cursor-pointer border-b border-slate-100 hover:bg-slate-50 ${
                          isSelected ? "bg-sky-50" : ""
                        }`}
                        onClick={() => selectAccession(f.accession_number)}
                      >
                        <td className="px-2 py-1 text-slate-700">{f.date_of_report}</td>
                        <td className="px-2 py-1">
                          {f.items.map((i) => (
                            <span
                              key={i.item_code}
                              className="mr-1 rounded bg-slate-100 px-1 py-0.5 text-[10px]"
                            >
                              {i.item_code}
                            </span>
                          ))}
                        </td>
                        <td className="px-2 py-1">
                          <span
                            className={`rounded px-1.5 py-0.5 text-[10px] font-medium ${SEVERITY_TONE[sev] ?? SEVERITY_TONE.low}`}
                          >
                            {sev}
                          </span>
                        </td>
                        <td className="px-2 py-1 text-slate-700">
                          {f.items.map((i) => i.item_label).join(" · ")}
                        </td>
                      </tr>
                    );
                  })}
                  {filtered.length === 0 && (
                    <tr>
                      <td colSpan={4} className="px-2 py-4 text-center text-slate-500">
                        No filings match these filters.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
            <EightKDetailPanel filing={selected} />
          </div>
        )}
      </Section>
    </div>
  );
}
```

- [ ] **Step 2: Register route**

In `frontend/src/App.tsx`, near the existing `Tenk10KDrilldownPage` registration:

```tsx
import { EightKListPage } from "@/pages/EightKListPage";

// inside <Routes>:
<Route
  path="/instrument/:symbol/filings/8-k"
  element={<EightKListPage />}
/>
```

- [ ] **Step 3: Run typecheck**

Run: `pnpm --dir frontend typecheck`
Expected: 0 errors.

- [ ] **Step 4: Commit**

```bash
git add frontend/src/pages/EightKListPage.tsx frontend/src/App.tsx
git commit -m "feat(#559-phase4): /filings/8-k filterable list page"
```

## Task 4.4: Vitest

**Files:**
- Create: `frontend/src/pages/EightKListPage.test.tsx`

- [ ] **Step 1: Write the test**

```tsx
import { describe, expect, it, vi } from "vitest";
import { render, screen, fireEvent, waitFor } from "@testing-library/react";
import { MemoryRouter, Route, Routes } from "react-router-dom";
import { EightKListPage } from "@/pages/EightKListPage";
import * as api from "@/api/instruments";

const filings = [
  {
    accession_number: "acc-1",
    document_type: "8-K",
    is_amendment: false,
    date_of_report: "2026-03-15",
    reporting_party: "GameStop Corp.",
    signature_name: "X",
    signature_title: "Y",
    signature_date: "2026-03-15",
    primary_document_url: null,
    items: [
      { item_code: "5.02", item_label: "Departure of Officer", severity: "high", body: "CFO out." },
    ],
    exhibits: [],
  },
  {
    accession_number: "acc-2",
    document_type: "8-K",
    is_amendment: false,
    date_of_report: "2025-12-04",
    reporting_party: "GameStop Corp.",
    signature_name: "X",
    signature_title: "Y",
    signature_date: "2025-12-04",
    primary_document_url: null,
    items: [
      { item_code: "8.01", item_label: "Other events", severity: "low", body: "Dividend." },
    ],
    exhibits: [],
  },
] as never;

describe("EightKListPage", () => {
  it("filters by severity and selects an accession via row click", async () => {
    vi.spyOn(api, "fetchEightKFilings").mockResolvedValue({
      symbol: "GME",
      filings,
    } as never);

    render(
      <MemoryRouter initialEntries={["/instrument/GME/filings/8-k"]}>
        <Routes>
          <Route
            path="/instrument/:symbol/filings/8-k"
            element={<EightKListPage />}
          />
        </Routes>
      </MemoryRouter>,
    );

    expect(await screen.findByText("CFO out.")).not.toBeNull;
    fireEvent.change(screen.getByLabelText("Severity"), { target: { value: "high" } });
    await waitFor(() => {
      expect(screen.queryByText("8.01")).toBeNull();
    });
    fireEvent.click(screen.getByText("2026-03-15"));
    expect(screen.getByText(/0001|acc-1/)).toBeTruthy();
  });
});
```

- [ ] **Step 2: Run**

Run: `pnpm --dir frontend test -- EightKListPage`
Expected: PASS.

- [ ] **Step 3: Commit**

```bash
git add frontend/src/pages/EightKListPage.test.tsx
git commit -m "test(#559-phase4): EightKListPage filter + select"
```

## Task 4.5: Wire `FilingsPane` 8-K row → list page

**Files:**
- Already wired in Task 3.1's `drilldownLink` (8-K family routes to `/filings/8-k`). No code change needed if Phase 3 merged before Phase 4 — confirm in pre-merge.

- [ ] **Step 1: Verify**

```bash
grep -n "filings/8-k" frontend/src/components/instrument/FilingsPane.tsx
```
Expected: route returns `/instrument/${symbolEnc}/filings/8-k`.

If Phase 4 lands before Phase 3 (allowed by spec), this task is a no-op until both are merged. Note the dependency in the PR description.

## Task 4.6: Pre-push gate + Codex + push + merge

- [ ] **Step 1: Full gate**

```bash
uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest && pnpm --dir frontend typecheck && pnpm --dir frontend test:unit
```

- [ ] **Step 2: Codex review**

Run: `codex.cmd exec review`

- [ ] **Step 3: Push + PR**

```bash
git push -u origin feature/559-phase4-eight-k-page
gh pr create --title "feat(#559-phase4): /filings/8-k filterable detail page" --body "$(cat <<'EOF'
## What

- New `/instrument/:symbol/filings/8-k` route (table + filters + detail panel).
- Severity / item-code / date-range filters, URL-deep-linkable.
- Reuses existing `/eight_k_filings` endpoint — no backend change.

## Why

Phase 4 of the density-grid spec — moves 8-K rendering off the inline panel into a dedicated workspace.

## Test plan

- [ ] Vitest: filter behaviour + row selection.
- [ ] Manual: `/instrument/GME/filings/8-k`, change severity → list narrows; click row → detail panel populates; deep-link `?accession=acc-1` selects on load.
EOF
)"
```

- [ ] **Step 4: Poll review + CI; resolve every comment; merge on APPROVE + green**

---

## Self-review summary

- **Spec coverage:** layout (Phase 3), 10-K drilldown three-pane + cross-refs + tables + history (Phase 2), 8-K page (Phase 4), `tables_json` schema + parser (Phase 1). All six brainstormed decisions covered.
- **Dependencies:** Phase 1 → Phase 2 (tables_json column + payload). Phase 3 + Phase 4 both depend on Phase 2 only for the 10-K drilldown link target — Phase 4 has zero schema dependency. Phase 3 + 4 can land in either order.
- **Out-of-scope (filed separately):** dup-quarter `financial_periods` (#558), Items 1A / 7 / 8 ingest, XBRL segments (#554), per-region 10-K equivalents (#516–#523), site-wide visual polish (#559's parent ticket retired in favor of this plan; visual polish lives at the new tech-debt ticket filed today).
