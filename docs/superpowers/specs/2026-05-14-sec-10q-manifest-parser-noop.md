# `sec_10q` manifest parser — synth no-op adapter

Date: 2026-05-14
Codex pre-spec round 1: 3 BLOCKING → pivoted from raw-fetch-no-op to true no-op (this revision).

## Background — what the gap actually is

Gap G4 in `.claude/skills/data-engineer/etl-endpoint-coverage.md` was historically attributed to **#414** ("Fundamentals ingest redesign"). Audit of #414's body + the 2026-04-28 scope-trim comment shows that ticket is about the `fundamentals_sync` cron's blocking behaviour during XBRL seed — **not** about wiring a `sec_10q` manifest parser. The "10-Q parser owned by #414" line in `[[us-source-coverage]]` and `etl-endpoint-coverage.md:46` is stale lineage and is re-pointed at the new ticket this spec opens.

What 10-Qs actually carry:

1. **XBRL financial facts** — quarterly revenue / EPS / balance sheet / cash flow. Already ingested via `data.sec.gov/api/xbrl/companyfacts/CIK*.json` by `fundamentals_sync` (Stage 24 + daily cron). Every Fundamentals metric in the catalog (`.claude/skills/metrics-analyst/SKILL.md` §2) lands per-quarter regardless of whether we parse the 10-Q HTML.
2. **Filing metadata** — accession, filed_at, period_of_report. Already populated in `filing_events` by the legacy submissions ingest path. "Last 10-Q date" renders today (`metrics-analyst SKILL.md:361`).
3. **Narrative HTML body** — MD&A, risk-factor updates, legal proceedings, controls. **Not extracted anywhere.** No operator-visible metric in the catalog consumes it as of 2026-05-14.

What's currently broken:

- `sec_10q` rows accumulate in `sec_filing_manifest` (post-#1155 Layer 1/2/3 wiring populates them aggressively).
- No registered parser → worker debug-skips them → `skipped_no_parser_by_source['sec_10q']` grows.
- The operator's manifest backlog signal carries permanent noise (`/coverage/manifest-parsers.has_registered_parser=False` for sec_10q) that masks real lane-stuck conditions.

## Goal

Register a synth no-op `sec_10q` parser with the manifest worker that transitions 10-Q / 10-Q/A rows from `pending` → `parsed` **without fetching the primary document and without writing any payload**. The parser asserts "the manifest discovery row IS the audit for this source; XBRL Company Facts owns the per-period data; no per-filing HTML extraction is required in v1."

## Scope

**In:**

- `app/services/manifest_parsers/sec_10q.py` — new synth no-op adapter (~50 lines including docstring).
- `app/services/manifest_parsers/__init__.py` — import + `register()` call.
- `tests/test_manifest_parser_sec_10q.py` — adapter contract tests.
- `app/services/processes/param_metadata.py:259` — remove `sec_10q` from the "may debug-skip" help text (it now has a parser).
- `.claude/skills/data-engineer/etl-endpoint-coverage.md` — update §2 row 12 (`sec_10q`) + §7 G4 row.
- `.claude/skills/data-sources/sec-edgar.md` §11.5 — remove `sec_10q` from stranded table; cite this PR as exemplar of the synth no-op pattern.

**Out:**

- Any payload fetch (`fetch_document_text`) — explicitly NOT added; preserves the pinned `tests/test_fetch_document_text_callers.py` contract.
- Any `store_raw` call. No raw HTML archive in this PR.
- Any typed-table extraction (MD&A text, risk-factor diffs, item-level structuring).
- Any share-class fan-out (no per-instrument writes).
- Any Option C `(filed_at, source_accession)` gate (no per-instrument typed-table to overwrite).
- Any schema migration (`sec_10q` enum value already in `ManifestSource`; no new tables).
- Retirement of `fundamentals_sync` / Companyfacts XBRL path. That remains the financial-data SoT.
- Adjusting #414's scope. #414 stays as-is; this work closes G4 with a new ticket.

## Architecture rationale — why a synth no-op

Three options were considered. Codex pre-spec round 1 flagged the raw-only middle option as a contract violation; the true no-op is the only option that satisfies all invariants.

| Option | What it does | Verdict |
|---|---|---|
| **A. Synth no-op (CHOSEN)** | Mark manifest `parsed` with no fetch, no store_raw, no typed write. `requires_raw_payload=False`. | Cleanest. Matches sec-edgar §11.5's documented tech-debt pattern: "register a synth 'no-op tombstone' parser". Honours the `fetch_document_text` allow-list contract. Zero new disk / fetch cost. |
| B. Raw-only no-op (fetch + store_raw) | Fetch primary doc, archive HTML, mark parsed | **REJECTED.** Codex BLOCKING ×3: (1) violates pinned `fetch_document_text` caller allow-list; (2) prevention-log #470 says raw persistence "redundant, not audit" when SQL coverage complete (which it is via Companyfacts XBRL); (3) `raise_for_status()` runs before body is returned so 403/429/5xx bodies are dropped pre-persistence per prevention-log §544. |
| C. Drop `sec_10q` from `ManifestSource` enum | Manifest stops manifesting 10-Qs | Loses discovery telemetry. Conflicts with #1155's "manifest is single source of truth" direction. §11.5 explicitly favours synth no-op over enum removal. |

The synth no-op pattern is documented in sec-edgar §11.5 today **only** for `sec_n_csr` (the `sec_xbrl_facts` row notes its own tech-debt; the `sec_10q` row still says "Blocked on #414"). This PR is the first concrete application of the pattern AND simultaneously rewrites §11.5 so the table reflects the new reality. `sec_10q` exits the "stranded by design" table because it now has a registered parser; the §11.5 row count drops from "Three" to "Two", and a new exemplar callout names this PR's parser as the canonical reference for the pattern.

Exact §11.5 replacement:

```diff
  ### 11.5 Stranded ManifestSource entries

- `ManifestSource` enum (sql/118 CHECK + `app/services/sec_manifest.py:106`) lists 14 values. Three carry no manifest parser by design:
+ `ManifestSource` enum (sql/118 CHECK + `app/services/sec_manifest.py:106`) lists 14 values. Two carry no manifest parser by design:

  | Source | Why no parser |
  |---|---|
  | `sec_xbrl_facts` | XBRL Company Facts ingested via bulk Company Facts API path, NOT per-filing manifest dispatch. |
  | `sec_n_csr` | EdgarTools `FundShareholderReport` exposes only OEF iXBRL fund-level facts ... Tech-debt eligible: either remove from `ManifestSource` Literal + `_FORM_TO_SOURCE` map, OR register a synth no-op parser using the `sec_10q` shape (see [11.5.1 below](#1151-synth-no-op-parser-pattern-sec_10q)). |
- | `sec_10q` | Blocked on #414 — the fundamentals ingest redesign owns the 10-Q parser. |
+
+ ### 11.5.1 Synth no-op parser pattern (`sec_10q` exemplar)
+
+ `sec_10q` was historically listed above as "blocked on #414". Audit (2026-05-14) found #414's scope is the `fundamentals_sync` cron redesign — NOT a 10-Q manifest parser. The 10-Q's financial data already lands via Companyfacts XBRL; its narrative HTML has no operator-visible consumer in v1. The right fix was a synth no-op parser, not a fetcher — `app/services/manifest_parsers/sec_10q.py` is the canonical reference. The shape:
+
+ - `requires_raw_payload=False` (no payload).
+ - Parser body returns `ParseOutcome(status='parsed', parser_version='<source>-noop-v1')` without DB writes, fetches, or typed-table touches.
+ - Durability test (`tests/test_manifest_parser_sec_10q.py::test_parser_does_not_touch_db_or_fetch`) asserts via sentinel connection + monkeypatched `store_raw` / `fetch_document_text` that the parser stays a no-op.
+ - `register_parser('<source>', _parse_<source>, requires_raw_payload=False)` in the source's `register()` callable.
+
+ Eligible adoptions: `sec_xbrl_facts` (companyfacts API is the per-period writer; manifest rows can drain via synth no-op); `sec_n_csr` if the #918 spike returns INFEASIBLE.

  `finra_short_interest` is not stranded — split tickets #915 (bimonthly) + #916 (RegSHO daily) are open. Parent #845 closed.
```

The same shape can later be applied to `sec_xbrl_facts` (companyfacts-API path) and to `sec_n_csr` (if the #918 spike returns INFEASIBLE).

If a future MD&A / risk-factor consumer materialises, **that PR** adds the fetcher + the allow-list update + the SQL normalisation in lockstep — exactly per the prevention contract. Until that consumer exists, fetching is premature.

## Settled-decisions touched

- **§"Filing event storage"** (settled-decisions.md:84) — `filing_events` stores metadata; "if full raw filing text is needed later, use a separate table, not `filing_events`". This PR writes nothing to `filing_events` and nothing to `filing_raw_documents`. PRESERVED.
- **§"Fundamentals provider posture"** (settled-decisions.md:48) — Free regulated source only. US fundamentals via SEC XBRL Company Facts. PRESERVED: no fundamentals path change; the 10-Q financial data continues to land via Companyfacts XBRL.
- **§"CIK = entity, CUSIP = security"** (settled-decisions.md:384) — Share-class fan-out applies to entity-level data writes per instrument. This PR writes nothing per instrument. NOT APPLICABLE.
- **§"Process topology (#719)"** (settled-decisions.md:324) — Manifest worker runs in jobs process. The parser code lives in `app.services.manifest_parsers` and is imported by both API (`app/main.py`) and jobs (`app/jobs/__main__.py`) for registry-view consistency. PRESERVED.
- **§"Product-visibility pivot"** (settled-decisions.md:306) — every new ticket should answer YES to "Would the operator feel this moves the product closer to 'I can manage my fund from this screen'?" — YES, indirectly: removes a permanent noise source from the manifest backlog signal so real lane-stuck conditions surface clearly.

## Prevention-log entries that apply

- **#470 "Raw payload persistence scope narrowed"** (prevention-log.md:530) — raw persistence redundant when SQL coverage complete. 10-Q financial data is SQL-complete via Companyfacts XBRL → no raw persistence required. PRESERVED.
- **"Every structured field from an upstream document lands in SQL"** (prevention-log.md:1171 + `tests/test_fetch_document_text_callers.py`) — pinned `fetch_document_text` caller allow-list. This PR adds NO new caller. PRESERVED.
- **"Bare call after committed savepoint can split raw/manifest status"** (prevention-log.md:1251) — no committed savepoint in this PR (no `store_raw`). NOT APPLICABLE.
- **"Manifest parser parse-failure broad-except writes ingest-log on EVERY exception class"** (prevention-log.md:1257) — synth no-op has no parse step that can raise. NOT APPLICABLE.
- **"Manifest parser upsert exception must discriminate transient vs deterministic"** (prevention-log.md:1263) — no upsert in this PR. NOT APPLICABLE.
- **"Raw payload persistence must precede `raise_for_status()`"** (prevention-log.md:544) — no fetch in this PR. NOT APPLICABLE.

## Implementation — `app/services/manifest_parsers/sec_10q.py`

```python
"""sec_10q manifest-worker parser — synth no-op adapter.

10-Q financial-statement data already lands via the Companyfacts XBRL
path (``fundamentals_sync`` daily cron + Stage 24 bootstrap). 10-Q
narrative HTML (MD&A, risk-factors, controls) has no operator-visible
consumer in v1. The manifest discovery row IS the audit signal for
this source; no per-filing payload work is needed.

This adapter exists to drain ``sec_filing_manifest`` rows for
``source='sec_10q'`` cleanly so:

- ``/coverage/manifest-parsers`` reports ``has_registered_parser=True``
- ``WorkerStats.skipped_no_parser_by_source['sec_10q']`` stays at 0
- Real lane-stuck conditions surface against a clean baseline

If a future PR introduces an MD&A / risk-factor extraction consumer,
that PR adds the fetcher + the
``tests/test_fetch_document_text_callers.py`` allow-list update + the
SQL normalisation pathway in lockstep, per the
"Every structured field lands in SQL" prevention contract.

ParseOutcome contract:

* ``status='parsed'`` — always. The manifest row's existence proves
  the filing was discovered; no further per-filing work is in scope.
* No ``tombstoned`` branch — there is no failure mode that requires
  permanent discard of the manifest row. Even a missing
  ``primary_document_url`` is acceptable: a future MD&A PR would
  need to handle that case, but for the synth no-op the URL is
  unused.
* No ``failed`` branch — there is no DB write that can raise; there
  is no fetch that can raise.

Raw-payload invariant (#938): registered with
``requires_raw_payload=False`` — this is a synth source per
sec-edgar §11.5. The worker accepts ``parsed`` with
``raw_status=None``.

Pattern reference: sec-edgar §11.5 documents the "synth no-op
tombstone" parser as the recommended tech-debt fix for sources whose
SQL coverage is complete via another path (``sec_xbrl_facts``,
``sec_n_csr``). This PR is the first concrete application.
"""

from __future__ import annotations

import logging
from typing import Any

import psycopg

logger = logging.getLogger(__name__)

_PARSER_VERSION_10Q = "10q-noop-v1"


def _parse_sec_10q(
    conn: psycopg.Connection[Any],
    row: Any,  # ManifestRow — forward-ref to avoid circular import
) -> Any:  # ParseOutcome — forward-ref
    """Synth no-op: mark the row parsed without touching SEC or DB."""
    from app.jobs.sec_manifest_worker import ParseOutcome

    logger.debug(
        "sec_10q manifest parser: synth no-op for accession=%s "
        "(financial data lands via Companyfacts XBRL; no per-filing payload work in v1)",
        row.accession_number,
    )
    return ParseOutcome(
        status="parsed",
        parser_version=_PARSER_VERSION_10Q,
    )


def register() -> None:
    """Register the synth no-op parser with the manifest worker."""
    from app.jobs.sec_manifest_worker import register_parser

    register_parser("sec_10q", _parse_sec_10q, requires_raw_payload=False)
```

### Registration

`app/services/manifest_parsers/__init__.py`:

```python
from app.services.manifest_parsers import sec_10q as _sec_10q  # new
...
def register_all_parsers() -> None:
    _eight_k.register()
    _def14a.register()
    _sec_10k.register()
    _sec_10q.register()  # new — synth no-op
    _sec_13dg.register()
    _insider_345.register()
    _sec_13f_hr.register()
    _sec_n_port.register()
```

### Param-metadata help text update

`app/services/processes/param_metadata.py:259`:

```diff
- "Note: sec_xbrl_facts / sec_n_csr / sec_10q / "
- "finra_short_interest may resolve to zero triples if "
+ "Note: sec_xbrl_facts / sec_n_csr / finra_short_interest "
+ "may resolve to zero triples if "
  "data_freshness_index has no rows for that source, OR "
  "reset triples that the manifest worker then "
  "debug-skips (no parser registered yet). Operator-"
  "visible outcome is scope_triples=N + "
  "discovery_new=0 in the job log."
```

`sec_10q` exits the "no parser registered" list as of this PR. Operator now sees `parsed=N` for `sec_10q` rebuilds rather than `skipped_no_parser`.

## Five-step contract (sec-edgar §11.1) compliance

| # | Rule | How this PR satisfies |
|---|---|---|
| 1 | `register()` callable in `app/services/manifest_parsers/<source>.py` | `app/services/manifest_parsers/sec_10q.py::register` |
| 2 | Import + register in `__init__.py::register_all_parsers` | added |
| 3 | `requires_raw_payload=True` for payload-backed sources | NO — this is a synth no-op per §11.5. `requires_raw_payload=False` is correct because no payload is fetched or stored. |
| 4 | Wrap every DB write in `conn.transaction()` | No DB writes in the parser. NOT APPLICABLE. |
| 5 | Failed outcomes set `next_retry_at` explicitly | No failed branch. NOT APPLICABLE. |

Rule 3 deviation is acceptable per §11.5's explicit allowance for synth no-op parsers ("synth no-op tombstone" — by design no payload).

## Tests — `tests/test_manifest_parser_sec_10q.py`

Four scenarios. ~120 lines.

| Test | Manifest input | Expected outcome |
|---|---|---|
| `test_synth_no_op_marks_parsed` | pending 10-Q row, valid URL + instrument_id (via `record_manifest_entry` with `subject_type='issuer'`) | run via `run_manifest_worker(conn, source='sec_10q')`. Manifest row transitions to `parsed`, `parser_version='10q-noop-v1'`. Post-assert: zero `filing_raw_documents` rows for that accession; no rows in `instrument_business_summary` / `instrument_business_summary_sections` (sibling typed tables that COULD be written by accident). |
| `test_synth_no_op_handles_10qa_amendment` | pending 10-Q/A row | manifest=`parsed`. Same code path; no fallback semantics. |
| `test_register_all_parsers_includes_sec_10q` | (registry test) | `clear_registered_parsers()` → `register_all_parsers()` → assert `'sec_10q' in registered_parser_sources()`. The clear-first step is mandatory: a registry leak from a prior test would false-pass without `__init__.py` actually wiring `sec_10q`. |
| `test_parser_does_not_touch_db_or_fetch` (durability gate) | Invoke `_parse_sec_10q(sentinel_conn, fake_row)` directly. `sentinel_conn` is a stub whose `execute` / `cursor` / `transaction` methods all raise `AssertionError("synth no-op must not touch DB")`. Monkeypatch BOTH `app.services.raw_filings.store_raw` AND `app.services.manifest_parsers.sec_10q.store_raw` (defensive — module-local import path) with a sentinel that raises if called. Monkeypatch `app.providers.implementations.sec_edgar.SecFilingsProvider.fetch_document_text` with a sentinel that raises if called. | Parser returns `ParseOutcome(status='parsed', parser_version='10q-noop-v1')` without invoking any of the sentinels. The triple-block test forces any future contributor who adds `conn.execute(...)`, `store_raw(...)`, or `fetch_document_text(...)` to also update this test — surfacing the spec-revision + Codex-review requirement instead of silently regressing into the raw-only design Codex round 1 rejected. |

Total: 4 tests.

The `monkeypatch(..., raising=False)` form is used on the parser-module-local `store_raw` symbol because today the module doesn't import it; without `raising=False`, monkeypatch fails on an absent attribute. The defensive symbol-patch catches a future regression where someone adds `from app.services.raw_filings import store_raw` at the top of `sec_10q.py` and uses the local binding.

Fixtures: `ebull_test_conn` from `tests/fixtures/ebull_test_db.py`, `_seed_instrument` helper, `record_manifest_entry` to insert pending rows. Mirrors `tests/test_manifest_parser_sec_10k.py` shape but trimmed to the no-op surface. Note: `record_manifest_entry` enforces `subject_type='issuer' ⇒ instrument_id IS NOT NULL` ([app/services/sec_manifest.py:222-227](app/services/sec_manifest.py#L222-L227)), so "missing instrument_id" is not reachable via the helper and is NOT in the test matrix — the synth no-op parser doesn't read `instrument_id` regardless, so the case carries no behavioural coverage.

## Validation — what to smoke after merge

Per CLAUDE.md ETL DoD §8-11:

1. **Smoke panel (clause 8):** AAPL, MSFT, JPM, HD, GME — each has multiple 10-Q filings in the last 8-quarter horizon. Trigger `POST /jobs/sec_rebuild/run` with `{"source": "sec_10q"}` on dev DB. Expected outcome: matching `sec_filing_manifest` rows transition from `pending` → `parsed` with `parser_version='10q-noop-v1'`. NO `filing_raw_documents` rows created; no typed tables written.
2. **Cross-source verify (clause 9):** spot-check one accession on AAPL — fetch `https://data.sec.gov/submissions/CIK0000320193.json`, pick the latest `form='10-Q'` accession, confirm the matching `sec_filing_manifest` row has `ingest_status='parsed'` post-rebuild. Independent source: SEC EDGAR direct.
3. **Backfill executed (clause 10):** `POST /jobs/sec_rebuild/run` with `{"source": "sec_10q"}` resets matching `sec_filing_manifest` rows to `pending`; manifest worker drains them on the next tick. Drain cost is one DB UPDATE per row (no SEC fetch, no payload write).
4. **Operator-visible figure (clause 11):** the `/coverage/manifest-parsers` audit endpoint flips `has_registered_parser=True` for `sec_10q`; the per-source `skipped_no_parser_by_source['sec_10q']` counter is zero on the next worker tick.
5. **PR description records SHA per clause** — per CLAUDE.md ETL DoD §12.

Per operator direction 2026-05-13, clauses 8-11 verification is deferred to end-of-epic clean-test pass; this PR records the **coverage-parity argument** (no new operator-visible figure changes; no payload writes; no schema changes; no SQL coverage change) as the standalone per-PR gate.

## Risks

| Risk | Mitigation |
|---|---|
| Future contributor regresses to a raw-fetch design ("we should archive the HTML too") | `test_parser_does_not_touch_db_or_fetch` raises if `conn.execute` / `conn.cursor` / `conn.transaction` / `store_raw` / `fetch_document_text` is called. Forces spec-revision + Codex-review flow. The parser docstring + §11.5 reference make the design rationale explicit. |
| Operator confusion — "parsed" without payload looks like a bug | Param-metadata help-text update names the synth no-op path explicitly. Docstring on the parser explains the design. Future ops-docs / runbook update can name the pattern by name ("synth no-op manifest sources"). |
| If MD&A consumer materialises, this PR has to be partially undone | The undo is purely additive: a future PR adds a fetcher, swaps the parser body, and updates `requires_raw_payload=True`. The current PR's tests change (the "does not fetch" guard would be deleted). No data migration. No revert. |
| Stale `[[us-source-coverage]]` line about "owned by #414" + ambiguous attribution in sec-edgar §11.5 | Both updated in this PR. Sec-edgar §11.5 row for `sec_10q` will be removed and a new "concrete synth no-op example" reference added pointing at this PR. Coverage matrix §2 row 12 + §7 G4 row both repointed at the new ticket. |

## Out-of-scope follow-ups

- Full MD&A extraction (typed table) — open if a thesis-writer / news-sentiment consumer requires it.
- Risk-factor diff extraction (10-Q risk factors are deltas from 10-K) — open if operator-visible chart materialises.
- Apply the same synth no-op pattern to `sec_xbrl_facts` (companyfacts-API path) — eligible tech-debt; §11.5 already notes it.
- Apply the synth no-op pattern to `sec_n_csr` if the #918 re-spike returns INFEASIBLE.

## Acceptance

- New ticket opens, scope = this spec.
- Parser registered in `register_all_parsers()`.
- 4 tests pass (including the "does not touch DB or fetch" durability gate).
- `/coverage/manifest-parsers` flips `has_registered_parser=True` for `sec_10q`.
- `param_metadata.py:259` help text updated.
- Coverage matrix §2 row 12 + §7 G4 + sec-edgar §11.5 + memory entries updated.
- No new operator-visible chart; no schema migration; no SQL coverage change; no new `fetch_document_text` caller.
- DoD §8-11 deferred to end-of-epic per operator direction; coverage-parity argument is the standalone per-PR gate.

## References

- `.claude/skills/data-sources/sec-edgar.md` §11.1 (architecture rule) + §11.2 (horizon) + §11.5 (stranded list — pattern source).
- `.claude/skills/data-engineer/etl-endpoint-coverage.md` §2 row 12 + §7 G4 (to be updated).
- `tests/test_fetch_document_text_callers.py` (the pinned caller contract this PR explicitly does NOT amend).
- Memory: `[[us-source-coverage]]` line 28 + `[[873-manifest-worker-parser-rollout]]` "Remaining work" line 38 (both to be updated to point at the new ticket).
- Sibling spec for shape (full parser): `docs/superpowers/specs/2026-05-13-1151-10k-manifest-parser.md`.
- Settled-decisions §"Filing event storage" + §"Fundamentals provider posture" + §"Process topology" + §"Product-visibility pivot".
- Prevention-log #470 (raw persistence redundant when SQL complete) + "Every structured field lands in SQL".
- Codex pre-spec round 1 transcript: 3 BLOCKING, all resolved by pivot from raw-only to synth no-op.
- Codex pre-spec round 2 transcript: 1 BLOCKING (durability-test shape) + 3 WARNING (§11.5 wording overstated; registry-test missing `clear_registered_parsers`; impossible "missing instrument_id" case). All resolved in this revision: durability gate strengthened to sentinel-conn + module-local `store_raw` patch; §11.5 includes explicit replacement text; `clear_registered_parsers()` added to registry test; impossible cases dropped.
