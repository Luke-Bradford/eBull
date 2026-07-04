# DEF 14A Item 402 — executive-compensation extraction (Summary Compensation Table)

Issue: #1945 (child of #1913 raw-store extraction-completeness audit).
Operator decision 2026-07-04: **EXTRACT-MORE (option 1)**. `def14a_body` stays
KEEP-RAW; fund an Item 402 parser first (exec comp + pay-vs-performance →
thesis inputs for #1919). Items 404/407 follow as children once 402's pattern
is proven. No sweep until wanted-fields-captured is declared.

Status: **spec** (unshipped). Implementation is a follow-up PR against this spec.

---

## Source rule

Governed by **Regulation S-K Item 402** (17 CFR § 229.402). The Summary
Compensation Table (SCT) is the prescribed disclosure; its structure is fixed
by the reg, so header-scored table detection (not first-principles column
guessing) is the correct parse strategy.

- **17 CFR § 229.402(c)(1)** — the SCT covers *"each of the registrant's last
  three completed fiscal years"* (one row per named executive officer per
  fiscal year → up to 3 rows/NEO).
- **17 CFR § 229.402(c)(2)(i)–(x)** — the prescribed columns, in order:

  | Col | § | Field |
  |-----|---|-------|
  | 1 | (c)(2)(i)    | Name and principal position |
  | 2 | (c)(2)(ii)   | Fiscal year |
  | 3 | (c)(2)(iii)  | Salary ($) |
  | 4 | (c)(2)(iv)   | Bonus ($) |
  | 5 | (c)(2)(v)    | Stock awards ($) — grant-date FV per FASB ASC 718 |
  | 6 | (c)(2)(vi)   | Option awards ($) — grant-date FV per FASB ASC 718 |
  | 7 | (c)(2)(vii)  | Non-equity incentive plan compensation ($) |
  | 8 | (c)(2)(viii) | Change in pension value & NQDC earnings ($) |
  | 9 | (c)(2)(ix)   | All other compensation ($) |
  | 10 | (c)(2)(x)   | Total ($) |

- **17 CFR § 229.402(a)(3)** — "named executive officers" (NEO) = the PEO (all
  who served), the PFO (all who served), the **three** most highly compensated
  executive officers other than PEO/PFO, plus **up to two** additional
  individuals who would have qualified but were not serving at fiscal year-end.
  Typical count 2–7.
- **17 CFR § 229.402(n)** (smaller reporting companies) — **scaled** SCT:
  **two** fiscal years and a reduced column set (the standalone "Change in
  pension value & NQDC earnings" column is not part of the SRC table; such
  amounts fold into "All other compensation"). The parser must not assume all
  10 columns are present. **Column resolution is strictly by matched header
  text, never by fixed positional index** — this is what makes the SRC variant
  safe without special-casing: an SRC table simply lacks the pension/NQDC
  header, so `pension_nqdc` resolves to NULL and the SRC's broader "All other
  compensation" lands in `other_comp`. No column is force-mapped. The exact
  SRC column enumeration (402(n)(2)(i)–(ix)) is a **verify-at-implementation**
  item against a real SRC panel filing — do not hard-code positional SRC
  columns from memory.

Parse hazard from the source form (not the reg): the string
`"Summary Compensation Table"` appears in **prose cross-references** ("…see the
Summary Compensation Table under…") *before* the actual `<table>` (illustrated
by accession `0000003545-25-000018`, where the first phrase hit is a
plan-description paragraph). This is a fixture illustration, not the safety
basis. **Safety rests on header scoring** of the `<table>` blocks in the section
window — the same `_score_table_headers` mechanism already proven at full
population by the Item 403 ownership engine — never on the first phrase hit.

---

## Full-population verification (dev DB, 2026-07-04)

Falsifies the naive framing in the #1913 audit ("29 GB / 42,289 rows of proxy
comp to mine"). The addressable Item 402 population is ~8 K, not 42 K.

`filing_raw_documents WHERE document_kind='def14a_body'` (scan capped at
`left(payload, 4 MB)` — one payload is 328 MB and OOMs an unbounded `~*`):

| Metric | Count | % |
|--------|-------|---|
| Total `def14a_body` rows | 42,289 | 100% |
| Contain `"summary compensation table"` | 7,999 | 18.9% |
| Contain SCT header `"name and principal position"` | 4,125 | 9.8% |
| Contain `"pay versus performance"` (Item 402(v)) | 6,202 | 14.7% |

Why 81% have no SCT — **not** truncation of our raw store, but filing mix. The
governing trigger is **Schedule 14A (17 CFR § 240.14a-101) Item 8**, which
requires the Item 402 disclosures only when the proxy solicits action on the
election of directors / executive-compensation matters. Non-SCT bodies are
DEFA14A additional soliciting material, DEFM14A merger proxies, DEFR14A
revisions, and notice-only/special-meeting proxies where Item 8 is not
triggered. The byte-size split below is corroborating evidence of that filing
mix, not the basis for the claim:

- Non-SCT rows: **p50 byte_count = 24 KB** (thin soliciting/notice docs).
- SCT-bearing rows: **p50 byte_count = 1.4 MB** (full annual proxies).

Control for filing type (the measured coverage bound) — of the **3,459**
accessions that already produced Item 403 beneficial-ownership rows (the
annual-proxy subset we parse today), **3,277 (94.7%)** also contain the SCT. So
Item 402 coverage is empirically ~1:1 with the ownership-parse cohort; the same
body pass that yields Item 403 yields Item 402 for the same filings.

**Coverage claim for the PR:** ~7,999 payloads are addressable; expected
first-pass yield is bounded by parse robustness against heterogeneous HTML, not
by raw-store gaps. Coverage is naturally bounded to comp-voting proxies — this
is expected, not a defect. The PR must `log()` the parsed-vs-addressable count
(no silent truncation).

---

## Schema

New typed table, mirroring `def14a_beneficial_holdings` (sql/097) conventions.
One row per **(accession, executive, fiscal_year)** — the SCT's natural grain.

```sql
-- sql/215_def14a_exec_compensation.sql  (next free number is 215)
CREATE TABLE IF NOT EXISTS def14a_exec_compensation (
    comp_id             BIGSERIAL PRIMARY KEY,
    instrument_id       BIGINT REFERENCES instruments(instrument_id),  -- nullable, CIK-resolved post-parse (mirror 097)
    accession_number    TEXT NOT NULL,
    issuer_cik          TEXT NOT NULL,
    executive_name      TEXT NOT NULL,
    principal_position  TEXT,                 -- (c)(2)(i) role portion, free-text (mirror holder_role: no CHECK)
    fiscal_year         INTEGER NOT NULL,     -- (c)(2)(ii)
    salary              NUMERIC(18, 2),       -- USD; (c)(2)(iii)
    bonus               NUMERIC(18, 2),       -- (c)(2)(iv)
    stock_awards        NUMERIC(18, 2),       -- (c)(2)(v)
    option_awards       NUMERIC(18, 2),       -- (c)(2)(vi)
    non_equity_incentive NUMERIC(18, 2),      -- (c)(2)(vii)
    pension_nqdc        NUMERIC(18, 2),       -- (c)(2)(viii) — NULL for SRC scaled SCT
    other_comp          NUMERIC(18, 2),       -- (c)(2)(ix)
    total_comp          NUMERIC(18, 2),       -- (c)(2)(x)
    fetched_at          TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Identity: (instrument_id, accession, executive, fiscal_year) — a plain
-- UNIQUE, mirroring the LIVE holdings key
-- uq_def14a_holdings_instrument_accession_holder (instrument_id,
-- accession_number, holder_name) [verified in dev: a later migration
-- superseded sql/097's 2-col index]. instrument_id is included because DEF 14A
-- fans out to sibling instruments via _resolve_siblings
-- (manifest_parsers/def14a.py:115); omitting it collapses a dual-class /
-- sibling issuer's comp rows. Excludes position (heuristic, like holder_role
-- in 097) so a re-parse with better role inference UPSERTs.
-- instrument_id stays nullable in DDL for audit parity with 097, but in
-- practice the manifest parser resolves CIK→instrument before writing (dev:
-- 0/38,607 holdings rows have NULL instrument_id), so a plain UNIQUE — not a
-- partial index — matches live behaviour and the upsert's ON CONFLICT target.
CREATE UNIQUE INDEX IF NOT EXISTS uq_def14a_comp_iid_acc_exec_fy
    ON def14a_exec_compensation (instrument_id, accession_number, executive_name, fiscal_year);

-- Hot path: latest comp for one instrument.
CREATE INDEX IF NOT EXISTS idx_def14a_comp_instrument_fy
    ON def14a_exec_compensation (instrument_id, fiscal_year DESC);
```

- `NUMERIC(18, 2)` — dollar amounts, cents precision. (Item 402 reports whole
  dollars but keep 2 dp for arithmetic-clean aggregation; never float.)
- **Ingest-log unchanged in v1.** `def14a_ingest_log` (sql/097) carries only
  `rows_inserted/rows_skipped/status/error` with a `status` CHECK of
  success/partial/failed — it cannot express a comp-specific count without a
  migration, and its status is holdings-driven. Do NOT overload it. Comp yield
  (parsed vs addressable) is surfaced via a structured `log()` line during the
  backfill drain (no silent truncation); if a persistent comp counter is later
  wanted it is a separate migration, not a v1 overload. A body that yields Item
  403 rows but no SCT leaves `status` untouched (comp absence is expected for
  many bodies — see Open questions).
- Update `_PLANNER_TABLES` in `tests/fixtures/ebull_test_db.py` in the same PR
  (prevention-log: new table must be registered or planner tests miss it).

## Parser

New pure function in `app/providers/implementations/sec_def14a.py`, reusing the
existing engine:

```python
@dataclass(frozen=True)
class Def14AExecCompRow:
    executive_name: str
    principal_position: str | None
    fiscal_year: int
    salary: Decimal | None
    bonus: Decimal | None
    stock_awards: Decimal | None
    option_awards: Decimal | None
    non_equity_incentive: Decimal | None
    pension_nqdc: Decimal | None
    other_comp: Decimal | None
    total_comp: Decimal | None

@dataclass(frozen=True)
class Def14ASummaryCompTable:
    rows: tuple[Def14AExecCompRow, ...]
    raw_table_score: int          # best header score, mirror Def14ABeneficialOwnershipTable

def parse_summary_compensation_table(html_text: str) -> Def14ASummaryCompTable: ...
```

Reuse verbatim (no fork): `_scan_outer_tables`, `_find_section_windows` (extend
its `_SECTION_HEADING_RE` to also anchor on `summary compensation table`),
`_parse_table_html`, `_is_inside_table`, `_strip_inline_html`,
`_parse_share_count`→a new `_parse_dollar` (SCT values carry `$` and commas;
share parser strips commas but not `$` — add a dollar variant),
`_looks_like_subheader`.

New, Item-402-specific:
- `_SCT_HEADER_KEYWORDS` — score a `<table>` by presence of `salary`, `bonus`,
  `stock awards`, `option awards`, `total`, `name and principal position`,
  `year`. Highest-scoring table in the SCT section window wins. Below a
  threshold → no SCT (log, don't guess).
- `_resolve_sct_columns(headers)` → map each of the 10 § (c)(2) fields to a
  column index **by matched header text**, `None` when absent (SRC scaled
  table). Never positional-fixed.
- Multi-row-per-NEO: the SCT lists a name once, then 2–3 fiscal-year rows. Carry
  the last-seen `executive_name`/`position` forward across year-only rows
  (mirror `_detect_role_heading`'s carry-forward). Fiscal year from col (ii).
- Footnote / total-row / director-table rejection: the Director Compensation
  table (Item 402(k)) and the Grants-of-Plan-Based-Awards table share layout;
  the header-score keyword set (must contain `salary`+`total`+name/position)
  discriminates SCT from those. Verify on the panel.

Cell-parsing edge cases the `_parse_dollar` helper + column resolver must
handle (verify each on the panel + fixtures):
- Split / multi-row headers (`Stock` / `Awards` across two `<tr>`s) — the SCT
  header is frequently two-deep; header collection must join stacked header rows
  before matching (the Item 403 engine's `_looks_like_subheader` is the model).
- `Non-Equity` vs `Nonequity` vs `Non Equity` hyphen/space variants; `NQDC`
  vs spelled-out; `Total` vs `Total ($)`.
- Empty / dash / em-dash / `—` / `N/A` cells → NULL, not 0.
- Parenthesised negatives `(1,234)` → negative (rare in SCT but present in
  pension-change / all-other).
- Trailing footnote markers on values (`450,000(3)`, superscripts) → strip
  before parse.
- `$` and thousands separators (share parser strips commas but not `$` — the
  new `_parse_dollar` strips both).
- "in thousands" / "reported in thousands" table captions → out of scope for
  v1 (SCT reports whole dollars per reg; flag + skip any table whose caption
  says thousands rather than silently scaling). Log if encountered.
- Foreign-currency footnotes (non-USD filers) → v1 assumes USD; flag + skip a
  clearly-non-USD SCT rather than store a wrong-denominated figure.

## Integration (one body pass; THREE call-sites share one version constant)

There are three code paths that read a `def14a_body` and write typed rows. Item
402 must be folded into **all three**, driven by ONE version bump. Do NOT
register a second `ParserSpec` — `registered_specs()` is one-spec-per-kind
(`rewash_filings.py:107`, `:643`).

1. **LIVE manifest-drain (primary path)** — `manifest_parsers/def14a.py`
   `_parse_def14a` (`:133`; upsert loop `:376`). This is what `sec_rebuild` and
   the normal fairness tick drive — NOT the legacy `_ingest_single_accession`.
   After `parse_beneficial_ownership_table(body)` (`:306`), also call
   `parse_summary_compensation_table(body)` on the same in-hand `body` and upsert
   comp rows. It already wraps `store_raw` in a savepoint (#938); put the
   **comp parse+upsert in its own savepoint** so a comp failure rolls back comp
   only and the holdings upsert (and the fairness tick) still commit — this is
   the failure-isolation requirement (savepoint = nested `with conn.transaction()`
   on a non-autocommit conn; prevention-log "savepoint≠commit").
2. **Rewash path** — `rewash_filings.py::_apply_def14a` (`:492`): under the same
   accession write lock, replace-then-insert comp rows alongside the holdings
   replace. A no-SCT on a body that previously had comp rows is a parser
   regression → raise `RewashParseError` (mirror the holdings contract).
3. **Legacy first-ingest** — `def14a_ingest.py::_ingest_single_accession`
   (`:682`): fold in for parity if still reachable (bootstrap/rescue). Reuse the
   existing `if not body:` falsy guard (prevention-log #1966 — `not`, not
   `is None`).

**Version bump — the real backfill driver.** The version stamped on drained
rows is `_PARSER_VERSION_DEF14A = "def14a-v1"` (`def14a_ingest.py:66`), consumed
by the manifest parser at every `store_raw`/`upsert` (`manifest_parsers/def14a.py`
passes it 8×) AND hard-coded again as `ParserSpec.current_version="def14a-v1"`
(`rewash_filings.py:644`). **Bump BOTH to `"def14a-v2"`** (ideally collapse the
duplicate literal — have `rewash_filings` import `_PARSER_VERSION_DEF14A` so they
can't drift). Per settled-decisions.md:96 the bump flows through `known_to`
supersession; the manifest re-evaluates the `def14a_body` cohort against the new
version → the ~8 K SCT-bearing bodies rewash and backfill comp automatically.
**This is the backfill mechanism — no separate migration backfill script.**

**No deterministic-error retry loop** (prevention-log #1131): constraint / parse
errors are terminal (`status='failed'`/`partial`, no 1h backoff re-fetch).

## Backfill & operator follow-up (runbook)

Parser change on `def14a_body` → per `.claude/CLAUDE.md` operator runbook:

1. Merge with version bump `def14a-v2`.
2. `POST /jobs/sec_rebuild/run` body `{"source": "sec_def14a"}` (or the
   `def14a_body` scope) — resets scheduler + manifest rows to `pending`; the
   manifest worker (10 req/s shared) drains re-reads from the stored body (no
   SEC fetch — KEEP-RAW payload is present).
3. Restart the jobs daemon onto new main (parser change) — graceful SIGTERM,
   confirm old PID gone (memory: I own jobs restarts after parser merges).
4. Wait for drain; monitor pending count for the scope → 0.
5. Verify operator-visible figure (see below).

## Verification plan (ETL DoD clauses 8–12)

- **Smoke panel** (clause 8): AAPL, GME, MSFT, JPM, HD — confirm CEO total_comp
  for the latest FY renders and is non-null for each.
- **Cross-source** (clause 9): spot-check one issuer's CEO total against the SEC
  EDGAR proxy directly (or the SCT in the filing's own HTML). E.g. AAPL Tim Cook
  FY total vs the DEF 14A as filed.
- **Backfill executed** (clause 10): the `sec_rebuild` invocation above, on dev
  DB, with drain-to-zero recorded.
- **Operator-visible** (clause 11): the comp data surfaces where a consumer
  reads it. **v1 scope = typed store + a read endpoint** (e.g.
  `/instruments/{symbol}/exec-compensation`); the thesis-engine consumer (#1919)
  and any instrument-page comp card are follow-ups. PR records the endpoint hit.
- PR records commit SHA per clause (clause 12).

## Settled decisions preserved

- `def14a_body` stays a **re-read** kind (settled-decisions.md:110). KEEP-RAW,
  not swept — matches operator EXTRACT-MORE. It is NOT added to
  `raw_filings.SWEPT_DOCUMENT_KINDS`.
- Single `ParserSpec` per `document_kind` (settled-decisions.md class model) —
  respected by folding into the existing spec + version bump, not a new kind.
- Parser-version bump orthogonality / `known_to` supersession
  (settled-decisions.md:96) is the backfill vehicle.

## Prevention-log ties

- #1966 — falsy-body guard `if not body:` (def14a_ingest.py listed as latent).
- #1131 — no deterministic-error 1h-retry loop in the comp parse path.
- #1700 — per-section failure isolation; a comp-parse raise must not poison the
  worker connection (`InFailedSqlTransaction`) or abort the holdings upsert.
- #1659 / #1915 — **do not** infer comp treatment from first principles; the SCT
  structure is source-ruled (Item 402(c)(2)). This spec cites the reg per field.

## Decisions taken in this spec (were open; resolved post-Codex ckpt-1)

- **Ingest-log** — not overloaded; `status` stays holdings-driven, comp absence
  leaves it untouched. Comp yield via `log()`. (Codex MED.)
- **SRC 402(n)** — no special-casing; strict header-mapping makes the absent
  pension/NQDC column resolve to NULL and folds SRC "all other" into
  `other_comp`. Exact SRC column enumeration verified at impl on a panel SRC.
  (Codex HIGH.)
- **Live path + version** — wire into `manifest_parsers/def14a.py` (primary) +
  rewash + legacy; bump `_PARSER_VERSION_DEF14A` (both literals). (Codex HIGH.)
- **Unique key** includes `instrument_id` (fanout). (Codex MED.)

## Open questions (for operator / implementation)

1. **Position-title normalisation** — store raw `principal_position` free-text
   (v1, mirror `holder_role`), defer CEO/CFO canonicalisation to the reader?
   Proposal: yes, raw in v1; the thesis consumer classifies.
2. **Multi-year rows vs latest-only** — store all up-to-3 FY rows (full SCT
   grain) vs latest FY only? Proposal: store all — the 3-FY trend is thesis
   signal (comp growth vs performance) and the grant is cheap.
3. **Pay-vs-Performance table (Item 402(v))** — present in 6,202 bodies; a
   distinct prescribed table (CAP vs TSR). Defer to a child ticket (like
   404/407) or fold now? Proposal: defer — 402(c) SCT first, prove the pattern.
