# NT 10-K / NT 10-Q late-filing parser (Form 12b-25)

Issue #1015 item 1 (child of #1011 PR4). Upgrades `NT 10-K` / `NT 10-Q` from
metadata-only to PARSE+RAW: a new manifest source `sec_nt` that fetches the
Form 12b-25 body and extracts the late-filing reason + restatement/guidance
signal.

## Source rule

**SEC Form 12b-25 / Rule 12b-25 (17 CFR 240.12b-25).** A registrant unable to
file a periodic report on time files Form 12b-25 within one business day of the
due date. Cover form is `NT 10-K` (annual) / `NT 10-Q` (quarterly); the document
is always Form 12b-25. Verified on two real filings (DOMO `0001628280-26-042197`,
QMCO `0001193125-26-270343`) — structure is fixed:

- **Cover** — "(Check one)" boxes: Form 10-K / 20-F / 11-K / 10-Q / 10-D /
  N-CEN / N-CSR. NT 10-K ⇒ 10-K box; NT 10-Q ⇒ 10-Q box.
- **"For Period Ended: \<date\>"** — period of the late report.
- **PART I — Registrant Information** (name, address).
- **PART II — Rules 12b-25(b) and (c)** — checkbox: registrant seeks relief
  (the late report "will be filed on or before the fifteenth calendar day"
  [10-K] / "fifth calendar day" [10-Q] following the due date). This is the
  Rule 12b-25(b) grace period: **15 days for annual, 5 days for quarterly.**
- **PART III — NARRATIVE** — "State below in reasonable detail why \[the report\]
  could not be filed within the prescribed time period." → **reason text.**
- **PART IV — OTHER INFORMATION** — item (3): "Is it anticipated that any
  significant change in results of operations from the corresponding period for
  the last fiscal year will be reflected …?" Yes/No + narrative/quantitative
  explanation. This is an **anticipated-results-change** signal (earnings
  direction vs the prior-year period) — it is NOT a restatement disclosure.

Checkbox encoding is NOT stable across the 2016→2026 population: EDGAR HTML
encodes a checked box as Unicode (`☒` U+2612 / `☑` U+2611), an HTML entity, or a
table-positioned `x`/`X`; empty is `☐` U+2610 or blank. The extractor must
tolerate all of these, and every checkbox-derived boolean is **nullable** —
when the box state can't be determined unambiguously the field is NULL, never a
guessed value. Glyph hit-rate is verified on a ≥30-filing population sample
during implementation (per "verify the signal on the full population"), not on
the two grounding fixtures.

## Premise check (dev DB, full population)

`filing_events`: 2 600 `NT 10-Q` + 1 648 `NT 10-K` (+ 51 `NT 20-F`, minor
others), 2016→2026. Today metadata-only — `red_flag_score=0.7` is already set at
ingest by `app/services/filings_risk.py` (`NT_LATE_FILING_SCORE`, regex
`^NT[ /-]`), so the *badge* already shows; this parser adds the **body content**
(why late, restatement flag) the operator currently can't see. NT rows are NOT
in `sec_filing_manifest` — `_FORM_TO_SOURCE` has no NT entry — so this introduces
a brand-new manifest source end-to-end (mirrors `sec_n_csr` #1172).

Scope of this PR: `NT 10-K`, `NT 10-Q` only (the issue's priority-1 forms). The
`/A` variants (2 rows total) and `NT 20-F` (foreign, different deadline regime)
are out of scope — left metadata-only, noted in the source doc.

## Extracted fields (`nt_filing_notices`)

| column | source | null? |
|--------|--------|-------|
| `accession_number` (PK) | manifest | no |
| `instrument_id` | manifest | no |
| `late_form` | derived from cover form: `NT 10-K`→`10-K`, `NT 10-Q`→`10-Q` | no |
| `period_of_report` (DATE) | "For Period Ended:" OR "For the Transition Period Ended:" | yes (unparseable) |
| `is_transition_report` (BOOL) | a "Transition Report on Form …" cover box is checked | no (default false) |
| `grace_period_days` | 15 if `10-K` else 5 (Rule 12b-25(b)) | no |
| `reason_text` (TEXT) | Part III narrative | yes (truncated/absent) |
| `results_change_anticipated` (BOOL) | Part IV (3) Yes/No box | yes (ambiguous → NULL) |
| `results_change_explanation` (TEXT) | Part IV (3) explanation when Yes | yes |
| `parser_version` | const | no |
| `parsed_at` | NOW() | no |

`late_form` is derived from the manifest form, NOT the cover checkbox — the form
type is authoritative and avoids brittle box-association on the cover. Cover-box
disagreement is logged but not fatal.

`seeks_grace_relief` is **deliberately omitted**: the Part II Rule 12b-25(b)
relief box is checked in nearly every NT filing (low discriminating signal) and
its position varies (before/after the `(a)` clause), making glyph association
fragile. `grace_period_days` (deterministic 15/5 from `late_form`) carries the
useful, reliable part. Period parsing reads the transition line as a fallback so
transition-report NT filings (separate Form 12b-25 boxes) aren't dropped.

## Schema

- `sql/208_nt_filing_notices.sql` — new table; PK `accession_number`, FK-free
  `instrument_id` (BIGINT, indexed), columns above. Mirrors `eight_k_filings`
  shape. The SAME migration widens THREE source/kind CHECK constraints for the
  new source (verified present on dev DB):
  - `filing_raw_documents_document_kind_check` += `'nt_body'` (10 → 11 kinds).
  - `sec_filing_manifest_source_check` += `'sec_nt'`.
  - `data_freshness_index_source_check` += `'sec_nt'`.
  NT bodies are small (~3–8 KB) → retained, NOT added to `SWEPT_MANIFEST_SOURCES`.

## Parser

- `app/services/nt_notices.py` — **pure** extractor `parse_nt_notice(html, late_form)
  -> NtNotice | None` (None ⇒ not a recognizable Form 12b-25 ⇒ tombstone) +
  `upsert_nt_notice(conn, instrument_id, accession, notice)`. Pure-fn so it
  table-tests against the two real fixtures with no DB.
- `app/services/manifest_parsers/sec_nt.py` — `_parse_nt(conn, row) -> ParseOutcome`
  mirroring `eight_k.py`: pre-fetch gates (missing url/instrument_id →
  tombstone) → fetch via `SecFilingsProvider.fetch_document_text` → empty/non-200
  → tombstone, fetch-exception → failed(retry) → `store_raw(document_kind='nt_body')`
  in a savepoint → parse → None → tombstone → else `upsert_nt_notice`.
  `_nt_fetch_url` prefetch hook + `register("sec_nt", …, requires_raw_payload=True)`.
  Registered in `manifest_parsers/__init__.py`.

  **`raw_status` invariant (#938):** once `store_raw` succeeds the raw row
  exists, so EVERY subsequent outcome — parse-returns-None tombstone, parse
  exception, `upsert_nt_notice` failure — MUST carry `raw_status="stored"` (the
  parse-None/exception paths return a savepointed `failed`/`tombstoned`
  ParseOutcome with `raw_status="stored"`, never `absent`). This mirrors the
  8-K parser's Codex-pinned round-2 fix.

## Chokepoint registrations (source-to-sink smoke `tests/smoke/test_etl_source_to_sink.py`)

Every `ManifestSource` must be wired at each chokepoint or the smoke fails:

1. `app/services/filings.py` — `NT 10-K`/`NT 10-Q`: `SEC_METADATA_ONLY` →
   `SEC_PARSE_AND_RAW`.
2. `app/services/sec_manifest.py` — `ManifestSource` Literal += `"sec_nt"`;
   `_FORM_TO_SOURCE` += `NT 10-K`/`NT 10-Q` → `sec_nt`.
3. `app/services/capability_manifest_mapping.py` — add `sec_nt` to
   `_UNMAPPED_MANIFEST_SOURCES` (late-filing is an issuer red-flag *signal*, not
   a standing data-coverage capability — same treatment rationale as a future
   episodic signal; documented inline).
4. `app/services/data_freshness.py` — staleness threshold. NT is episodic (not
   periodic); use a generous cap so a clean filer never reads "stale" (e.g.
   400d, documented as episodic).
5. `app/services/processes/param_metadata.py` — `sec_rebuild` source enum +=
   `sec_nt`.
6. `app/jobs/sec_first_install_drain.py` — NT is small + episodic; seed
   `initial_ingest_status="pending"` (NOT deferred like the heavy 10-K/8-K
   bodies). Confirm no per-source branch assumes exhaustiveness.
7. `scripts/_etl_source_inventory.py` — extend the sink-kind taxonomy with a new
   `nt_notice` kind (the documented set is `{ownership_observation, fund_metadata,
   business_summary, eight_k, synth_noop}`); `MANIFEST_SOURCE_SINKS["sec_nt"] =
   (("nt_filing_notices",), "nt_notice")`.
8. `app/services/raw_filings.py` — `DocumentKind` Literal += `"nt_body"`, and add
   it to `KEPT_NEGLIGIBLE_DOCUMENT_KINDS` (every `DocumentKind` must be classed
   swept-or-kept; NT bodies are tiny → kept-negligible).
9. `tests/test_fetch_document_text_callers.py` — `_ALLOWED_CALLER_FILES` +=
   `"app/services/manifest_parsers/sec_nt.py"` (the call site is pinned).
10. `tests/smoke/test_etl_source_to_sink.py` — `_PARSER_MODULE_BY_SOURCE` +=
    `sec_nt` → its parser module (collection asserts parity with
    `MANIFEST_SOURCE_SINKS`).
11. `docs/etl/sources/sec_nt.md` — 13 required sections.

## Operator surface

`nt_filing_notices` LEFT JOINed in `GET /filings/{instrument_id}`
(`app/api/filings.py`): for NT rows, attach a `nt_notice` object
(`late_form`, `period_of_report`, `reason_excerpt` [first ~280 chars],
`results_change_anticipated`). The red-flag badge/trend already render via
`red_flag_score`. A dedicated "Problem panel" FE component is a **separate FE
child ticket** (out of scope here — backend is the unit of work).

## Backfill (split)

Going-forward discovery is wired by this PR (`_FORM_TO_SOURCE` map → submissions
ingest seeds new NT rows). The historical 4 248-row backfill (seed manifest from
existing NT `filing_events` + drain at 10 req/s, ~hours) is a **child ticket**
"NT notice backfill drive" — mirrors `sec_n_csr`'s split (#1174/#1176) and #755.
For DoD clauses 8–11 this PR seeds + drains a **bounded sample** (panel
AAPL/GME/MSFT/JPM/HD where present + a recent NT slice) on dev and verifies the
endpoint figure + one cross-source spot-check (SEC EDGAR direct).

## Tests

- `tests/test_nt_notices_parser.py` — pure: the two real fixtures (DOMO 10-Q
  results-change=No; QMCO 10-K), a malformed/empty body → None, a fixture with
  `☒ Yes` on item (3) → `results_change_anticipated=True` AND its explanation
  text extracted (boundary check), an ASCII-`x` checkbox variant → still
  detected, and an ambiguous/both-boxes case → `results_change_anticipated=None`.
  A transition-report fixture → `period_of_report` from the transition line +
  `is_transition_report=True`.
- `tests/test_manifest_parser_sec_nt.py` — integration mirroring
  `test_manifest_parser_eight_k.py`: happy path (fetch→store_raw→parse→upsert→
  parsed/stored), empty fetch → tombstone, fetch exception → failed+retry,
  parse-None → tombstone.

## Settled decisions / prevention log

- Settled "Official filings providers: SEC EDGAR is the official filings source
  for US issuers" + "prefer the official filing" — preserved (parsing the
  authoritative SEC document).
- Prevention "single chokepoint discipline" — extraction lives in
  `nt_notices.parse_nt_notice`, the manifest wrapper only orchestrates fetch/
  store/transition.
- Prevention "new ManifestSource must register at every chokepoint" — the
  source-to-sink smoke is the enforced checklist (above).
