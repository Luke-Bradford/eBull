# Dividend events extraction in 8-K manifest parser (#1158)

> Unblocks `sec_dividend_calendar_ingest` cron retirement (8th of 8 in
> the post-#1155 sweep). Companion retirement PR follows once this
> lands. See `[[legacy-cron-retirement]]` memory entry for the
> umbrella context.

## Problem

`sec_dividend_calendar_ingest` (`app/workers/scheduler.py:3685`) is
the sole writer of `dividend_events`. Its job: scan
`filing_events` for 8-K rows whose `items[]` contains `'8.01'`,
fetch the primary document via SEC, run regex extraction via
`app.services.dividend_calendar.parse_dividend_announcement`, and
upsert one row per `(instrument_id, source_accession)`.

The manifest worker has owned the steady-state 8-K path since #1126
via `app/services/manifest_parsers/eight_k.py`. That adapter writes
`eight_k_filings` + `eight_k_items` + `eight_k_exhibits`, but does
NOT call into the dividend extractor. So `dividend_events` has two
independent code paths — one through the manifest, one through the
legacy cron — and only the cron writes `dividend_events`.

Per the `[[legacy-cron-retirement]]` map, this is the only parser
gap blocking the umbrella retirement sweep.

## Settled-decisions check

- **Filing event storage** (`docs/settled-decisions.md` §"Filing
  event storage"): `dividend_events` is the canonical home for
  parsed dividend calendar facts. No schema change needed; the
  table already exists with the `(instrument_id, source_accession)`
  unique key.
- **Filing dedupe**: provider-scoped accession identity is stable
  per #434. The existing `(instrument_id, source_accession)` upsert
  contract is preserved.
- **CIK = entity, CUSIP = security (#1102, settled 2026-05-10).**
  Share-class siblings (GOOG/GOOGL etc) share an issuer CIK. The
  manifest path's anchor `instrument_id` is one sibling; the legacy
  cron writes one `dividend_events` row per `filing_events` row,
  so siblings each got their own row. The manifest path MUST fan
  out to share-class siblings via `_resolve_siblings` to preserve
  parity. See "Share-class fan-out" below.
- **Provider boundary**: parser stays a service-layer concern;
  `SecFilingsProvider` is unchanged.
- **Auditability**: `dividend_events.last_parsed_at` continues to
  bump on every write so operators can audit re-parse cadence.
- **Cancel UX (#1064)**: N/A — the dividend extraction runs inside
  one manifest-row tick (sub-second), no cooperative-cancel surface.

No settled decision is being changed.

## Review-prevention-log entries that apply

- **PR #1131 — upsert exception discrimination.** Any new path that
  upserts to a typed table from inside a manifest parser must
  classify the exception via
  `app.services.manifest_parsers._classify.is_transient_upsert_error`.
  `OperationalError` → 1h backoff retry; deterministic →
  log + drop the dividend-side write but PRESERVE the 8-K parsed
  outcome (rationale below).
- **PR #1126 — bare expression after committed savepoint must be
  wrapped in try/except.** The dividend extraction sits AFTER
  `upsert_8k_filing`'s savepoint commits; it must be wrapped so a
  dividend-side error doesn't abort the worker's outer transaction
  before `transition_status` fires.
- **PR #1132 — psycopg3 `conn.transaction()` inside an open tx is a
  SAVEPOINT, not a COMMIT.** The dividend extraction will run
  inside a fresh `with conn.transaction():` savepoint so its
  failure is isolated from the 8-K filing/items/exhibits writes
  that already landed.
- **PR #1152 — share-class fan-out fail-closed pattern.**
  `_resolve_siblings` returns `sorted(set(siblings) | {instrument_id})`
  so when `external_identifiers` is stale or missing for the
  canonical sibling, the manifest's anchor instrument_id stays in
  scope. Apply the same union here.
- **#1152 Option C filed_at gate**: N/A —
  `dividend_events` is keyed on `(instrument_id, source_accession)`,
  not `(instrument_id, period)`. Multiple accessions never collide
  on the same row, so there is no same-day-overwrite hazard. The
  upsert is naturally idempotent.

## Chosen option — Option A (fold extraction into eight_k.py)

Issue #1158 lays out three options; Option A is the recommendation
on the ticket and is the right call for this codebase today:

- **Option A — single-pass extraction.** Inside
  `_parse_eight_k`, after the existing `upsert_8k_filing` savepoint
  commits, run `parse_dividend_announcement(html)` on the SAME
  body the 8-K parser already fetched. If a non-`None` announcement
  is returned, fan out to share-class siblings and upsert
  `dividend_events`. Reuses the already-fetched body — no extra
  SEC call. Composite parser_version (manifest column only) pins
  the dividend version so a regex bump is detectable on
  operator-triggered `sec_rebuild`.

- **Option B (rejected).** Re-scope the legacy cron to read from
  `eight_k_items`. Decouples but leaves two scheduled jobs in the
  dividend path forever. Doesn't get us share-class fan-out for
  free either — the legacy cron's per-`filing_events` design
  already fans out by accident, but the manifest replacement would
  need its own fan-out logic anyway.

- **Option C (rejected).** Generic post-parser hook mechanism.
  Over-engineered for a single consumer. If a second 8-K-derived
  dataset emerges (cybersecurity-incident extraction, M&A signal
  extraction, etc.) we can lift Option A's pattern into a
  registered hook then. YAGNI for now.

## Implementation

### 1. Drop the item-code gate; let the regex be the gate

The legacy cron filters `filing_events.items[] @> ['8.01']` and
THEN runs `parse_dividend_announcement`. The manifest path does
not have access to the submissions-derived `items[]` array on the
manifest row — `parse_8k_filing(html, known_items=())` extracts
items from the HTML body itself, and there is real divergence
between submissions-declared items and HTML-extracted items
(filings sometimes declare 8.01 in submissions but lack the
"Item 8.01" heading in the HTML body, or vice versa).

Codex pre-spec checkpoint 1 BLOCKING: if the implementation gates
on `parsed.items` (the HTML-extracted set), filings that genuinely
carry a dividend announcement but lack an explicit "Item 8.01"
heading are silently dropped. Legacy coverage is lost.

Resolution: do NOT gate on items. Run
`parse_dividend_announcement(html)` unconditionally on every
parseable 8-K body. The pure-function parser already has a strict
internal gate (`_DIVIDEND_CONTEXT_RE`) requiring `$N.NN per share`
in proximity to the word `dividend`. False positives on
non-dividend 8-Ks are extremely unlikely; cost is one regex scan
per 8-K (cheap). Net effect: equal-or-better coverage than the
legacy cron, since amendments (8-K/A) and items-array-misclassified
filings now get extraction too.

### 2. Share-class fan-out (#1102 / #1117)

`dividend_events` is keyed `(instrument_id, source_accession)`.
The legacy cron iterates `filing_events` rows — siblings each
have their own `filing_events` row, so siblings each get their
own `dividend_events` row.

The manifest row carries a single anchor `instrument_id`. To
preserve sibling parity, fan out via `_resolve_siblings` from
`app.services.manifest_parsers.sec_10k:120-142` — moved into a
shared module if not already (see step 5 below).

```python
siblings = _resolve_siblings(
    conn,
    instrument_id=row.instrument_id,
    issuer_cik=(row.cik or "").strip() or _CIK_MISSING_SENTINEL,
)
for sibling_iid in siblings:
    upsert_dividend_event(
        conn,
        instrument_id=sibling_iid,
        source_accession=accession,
        announcement=announcement,
    )
```

The fail-closed union pattern (`set(siblings) | {instrument_id}`)
means the canonical sibling never disappears from scope if its
`external_identifiers` row is stale. This matches the sec_10k.py
canonical implementation at line 142.

### 3. Composite parser_version — manifest column only

Mirror the `sec_13f_hr.py:88-89` composite shape FOR THE MANIFEST
COLUMN ONLY:

```python
# In app/services/manifest_parsers/eight_k.py
from app.services.dividend_calendar import _PARSER_VERSION_DIVIDEND
from app.services.eight_k_events import _PARSER_VERSION  # already imported

_PARSER_VERSION_EIGHT_K = f"8k:{_PARSER_VERSION}+dividend:{_PARSER_VERSION_DIVIDEND}"
```

Replace `str(_PARSER_VERSION)` ONLY at the `ParseOutcome` /
`_failed_outcome` sites in `eight_k.py` (the manifest adapter) —
that's the value the worker stamps onto
`sec_filing_manifest.parser_version`. Specifically:

- `_failed_outcome` (current line 92) → composite.
- Every `ParseOutcome(... parser_version=str(_PARSER_VERSION))`
  site (currently at lines 138, 148, 187, 257, 312, 319) →
  composite.
- `store_raw(... parser_version=str(_PARSER_VERSION))` at
  line 205 → STAYS bare. That column is
  `filing_raw_documents.parser_version` — provenance for the
  cached HTML body, not a rewash signal. Tying raw-document
  versioning to dividend regex changes would invalidate the raw
  cache on every regex bump, forcing redundant SEC re-fetches
  with no integrity benefit. (Codex pre-spec round 2 HIGH.)

The typed `eight_k_filings.parser_version` column (written by
`upsert_8k_filing` + `_write_tombstone` in
`app/services/eight_k_events.py:425+, 514+`) is INTENTIONALLY
LEFT AT THE BARE `_PARSER_VERSION` integer. That column is
provenance for the typed-table writer and is not consumed by any
rewash logic — diverging it from the manifest column would force
threading parser_version into both writer signatures for no
operator-visible benefit. Both columns stay independently
maintained.

`_PARSER_VERSION_DIVIDEND` is a NEW module-level constant in
`app/services/dividend_calendar.py`, initialised to `1`. Bump it
when the regex tightens.

### 4. Operator-driven rewash (NOT automatic)

Codex pre-spec checkpoint 1 BLOCKING: there is NO automatic
parser_version-mismatch detector in the codebase.
`app/jobs/sec_rebuild.py::run_sec_rebuild` only resets manifest
rows for an explicit operator-supplied scope. The
`# Preserves parser_version (so the rewash detector can compare)`
comment at `sec_rebuild.py:131` refers to a future detector, not
an existing one.

Operator runbook for re-extracting dividend data after a regex
bump:

1. Bump `_PARSER_VERSION_DIVIDEND` in `dividend_calendar.py` (and
   land the regex change). Composite manifest version becomes
   `8k:N+dividend:M+1`.
2. Trigger `POST /jobs/sec_rebuild/run` with `{"source": "sec_8k"}`.
   Resets every 8-K manifest row to `pending`.
3. Manifest worker drains the backlog at the 10 req/s shared rate
   limit. Each accession re-fetches HTML, re-parses, re-runs
   dividend extraction with the new regex.
4. Newly-extracted `dividend_events` rows overwrite prior
   partial rows via `ON CONFLICT (instrument_id, source_accession) DO UPDATE`.

This is a more bounded, operator-controlled re-parse pattern than
the legacy cron's daily-+-7-day-TTL loop. Operators trigger
explicit recompute when regex changes; otherwise extraction is
one-shot per accession.

### 5. _resolve_siblings shared helper

`_resolve_siblings` currently lives at
`app/services/manifest_parsers/sec_10k.py:120`. Two options:

**Option a — copy verbatim.** Duplicate the function into
`eight_k.py`. Faster but creates drift risk.

**Option b — extract to `app/services/manifest_parsers/_siblings.py`**
shared module + import from both. Cleaner but one extra file.

Going with **Option b**: extract the helper plus the
`_CIK_MISSING_SENTINEL` constant into
`app/services/manifest_parsers/_siblings.py`. Update both
`sec_10k.py` and `eight_k.py` to import from the shared module.
The other
manifest parsers (`def14a.py`, `insider_345.py`, `sec_13f_hr.py`,
`sec_n_port.py`, `sec_13dg.py`) follow per-source fan-out
patterns that don't currently use `_resolve_siblings` — leave them
alone in this PR; future fan-out unification is its own ticket.

### 6. Extraction call site

Inside `_parse_eight_k`, AFTER the `upsert_8k_filing` savepoint
commits successfully and BEFORE the final `ParseOutcome(status='parsed')`
return, run:

```python
try:
    with conn.transaction():
        _maybe_extract_dividend(
            conn,
            instrument_id=instrument_id,
            issuer_cik=row.cik,
            accession=accession,
            html=html,
        )
except Exception as exc:
    if is_transient_upsert_error(exc):
        return _failed_outcome(format_upsert_error(exc), raw_status="stored")
    # Deterministic dividend-side failure must NOT block the 8-K
    # parsed outcome — the 8-K filing/items/exhibits writes already
    # landed and are correct. Log + drop the dividend extraction;
    # operator can re-trigger via parser_version bump or
    # sec_rebuild. Tombstoning the manifest row would lose
    # operator-visible 8-K data.
    logger.exception(
        "eight_k manifest parser: dividend extraction failed accession=%s",
        accession,
    )
```

`_maybe_extract_dividend` is a new private helper in `eight_k.py`:

```python
def _maybe_extract_dividend(
    conn: psycopg.Connection[Any],
    *,
    instrument_id: int,
    issuer_cik: str | None,
    accession: str,
    html: str,
) -> None:
    """Upsert dividend_events rows when the 8-K body parses
    non-trivially as a dividend announcement. Idempotent — a
    re-run on the same accession produces zero additional rows
    per sibling. No item-code gate (Codex pre-spec BLOCKING):
    the parser's internal _DIVIDEND_CONTEXT_RE is the gate."""
    announcement = parse_dividend_announcement(html)
    if announcement is None:
        # Non-dividend body (no `$N.NN per share` near `dividend`).
        # Item 8.01 covers buybacks / litigation / JVs; some
        # dividend announcements live under Item 7.01 / 8.01-but-
        # mis-headed. The regex gate handles all cases.
        return
    siblings = _resolve_siblings(
        conn,
        instrument_id=instrument_id,
        issuer_cik=(issuer_cik or "").strip() or _CIK_MISSING_SENTINEL,
    )
    for sibling_iid in siblings:
        upsert_dividend_event(
            conn,
            instrument_id=sibling_iid,
            source_accession=accession,
            announcement=announcement,
        )
```

`upsert_dividend_event` already exists at
`app/services/dividend_calendar.py:464` and handles the
`ON CONFLICT (instrument_id, source_accession) DO UPDATE` write.
Its boolean return value (`True` = INSERT, `False` = UPDATE) is
discarded here — the manifest worker only needs `parsed` /
`tombstoned` / `failed` granularity at the row level.

### 7. Idempotency contract

- `(instrument_id, source_accession)` is the upsert key per sibling
  (per `dividend_events` migration). Re-running on the same
  accession produces zero additional rows.
- A re-run with the same `_PARSER_VERSION_DIVIDEND` and identical
  HTML body produces an UPDATE per sibling with identical column
  values plus a bumped `last_parsed_at`. That bump is
  intentional — it lets operators audit re-parse cadence.
- Bumping `_PARSER_VERSION_DIVIDEND` causes the composite version
  string carried on `sec_filing_manifest.parser_version` to change.
  Operator-triggered `sec_rebuild` then re-pends every 8-K
  accession; the worker re-runs `eight_k.py` (with the new
  dividend regex) across the entire 8-K backlog at the manifest's
  10 req/s shared rate limit.

### 8. Interaction with `_classify.is_transient_upsert_error`

Same contract every other manifest parser uses (see
`eight_k.py:283-309` for the upsert_8k_filing version):

- `OperationalError` (psycopg3 base for `SerializationFailure` /
  `DeadlockDetected` / connection drop) → `_failed_outcome` with
  the standard 1h backoff. Worker re-fetches on the next tick.
- Anything else (`IntegrityError` / `DataError` /
  `ProgrammingError` / non-DB Python exceptions) → log + drop the
  dividend extraction but PRESERVE the `parsed` outcome for the
  8-K body itself. Different from the upsert_8k_filing path
  (which tombstones on deterministic failure) because:
  - The 8-K filing rows are the primary contract of this parser.
    Failing the entire manifest row over a dividend-extraction
    bug would silently lose 8-K visibility for that accession on
    the operator UI.
  - A deterministic dividend-extraction bug should be loud (logged
    `exception`) but recoverable via `_PARSER_VERSION_DIVIDEND`
    bump + `sec_rebuild` after the bug is fixed.

### 9. 8-K/A amendment behavior — improvement over legacy

Manifest's `sec_8k` source covers BOTH `8-K` and `8-K/A` form
codes (per `app/services/sec_manifest.py:817-818`). The legacy
cron only handles `filing_type = '8-K'` (per
`app/services/dividend_calendar.py:328-330`). After this PR,
amended 8-K filings (which restate dividend dates) will land
their own `dividend_events` rows under their amendment accession.

This is a STRICT IMPROVEMENT — restated dividend dates get into
the table. No overwrite hazard: amendments carry distinct
`source_accession` values, so they land as new rows rather than
overwriting the original-filing row.

### 10. Tombstone-on-parse-miss removed

The legacy cron writes a NULL-row tombstone on parse-miss and
fetch error. The 7-day TTL on `last_parsed_at` bounds re-fetch
cadence to weekly. After PR A:

- **Parse-miss (regex returns None):** no row written. The
  manifest row transitions to `parsed` so the worker doesn't
  re-fetch the same accession again until operator triggers
  rewash via `_PARSER_VERSION_DIVIDEND` bump + `sec_rebuild`.
  This is functionally equivalent to "weekly re-parse" but
  operator-controlled, not timer-driven.
- **Fetch error (HTTP 5xx, connection drop):** the WHOLE 8-K
  manifest row goes to `failed` with 1h backoff via
  `_failed_outcome` already. Worker retries the entire row
  including dividend extraction. Capped at 1h (vs legacy's 7-day
  TTL) — slightly higher SEC load if many filings are
  permanently 404, but bounded by the shared 10 req/s limit and
  by the existing manifest worker's backoff.

Trade-off: legacy 7-day TTL would re-parse partial rows
automatically when the regex tightens. Manifest path makes that
operator-driven. Net: tighter coupling between regex change and
re-parse, easier to reason about for ops, no silent retry loops.

## Test plan

### New tests in `tests/test_manifest_parser_eight_k.py`

1. **`test_dividend_extraction_writes_dividend_events`** — feed a
   fake 8-K HTML body with Item 8.01 + a complete dividend
   announcement (declaration, ex, record, pay, dps). Drive the
   manifest worker. Assert one row in `dividend_events` with
   matching dates / amount / `(instrument_id, accession)`, and the
   manifest row transitions to `parsed`.
2. **`test_dividend_extraction_runs_when_html_lacks_explicit_item_801_heading`**
   — feed an 8-K body with dividend language present but no
   "Item 8.01" heading. Assert dividend_events row written.
   (Codex pre-spec BLOCKING: covers the "items[] declares 8.01 but
   HTML doesn't" divergence the legacy cron handled by gating on
   submissions-derived items.)
3. **`test_dividend_extraction_no_dividend_language_skips`** —
   feed an 8-K body with Item 8.01 but no dividend language (e.g.
   buyback announcement). Assert manifest row → `parsed` AND zero
   `dividend_events` rows for the accession AND no tombstone row.
4. **`test_dividend_extraction_fans_out_to_share_class_siblings`**
   — seed two share-class siblings (e.g. `GOOG` + `GOOGL` with
   shared CIK via `external_identifiers`). Drive the manifest
   worker on one sibling's instrument_id. Assert `dividend_events`
   rows for BOTH siblings under the same accession.
   (Codex pre-spec BLOCKING.)
5. **`test_dividend_extraction_falls_back_to_anchor_instrument_when_siblings_resolve_empty`**
   — pre-#1102 fail-closed pattern: when `external_identifiers`
   is missing the canonical CIK row, `_resolve_siblings` returns
   the manifest's anchor `instrument_id` only. Assert at least
   one row written even on a stale-cik state.
6. **`test_dividend_extraction_idempotent_under_replay`** — drive
   the worker on the same accession twice. Assert exactly one
   `dividend_events` row per sibling, with `last_parsed_at`
   bumped on the second run.
7. **`test_dividend_extraction_deterministic_failure_preserves_8k_parse`**
   — monkeypatch `upsert_dividend_event` to raise
   `psycopg.errors.IntegrityError`. Assert the manifest row STILL
   transitions to `parsed` (8-K body wrote successfully) AND the
   dividend extraction is logged at exception level. The 8-K
   filing/items/exhibits rows must still land.
8. **`test_dividend_extraction_transient_failure_returns_failed_outcome`**
   — monkeypatch `upsert_dividend_event` to raise
   `psycopg.errors.OperationalError`. Assert ParseOutcome is
   `failed` with `next_retry_at` set to ~1h from now AND
   `raw_status='stored'`.
9. **`test_composite_parser_version_format`** — register the
   parser and inspect a successful ParseOutcome's `parser_version`
   — assert it matches the composite shape `8k:N+dividend:M`.

### Preserved tests

`tests/test_dividend_calendar.py` golden cases (`TestParseDividendAnnouncement.*`)
exercise the pure-function parser directly. No changes — the
extraction call site shifts from the legacy cron to `eight_k.py`,
but the pure-function contract is untouched.

`tests/test_dividend_calendar_ingest.py` exercises the legacy cron
service module. PR A keeps that module + the cron's call into it
intact (the retirement happens in PR B). Tests pass unchanged.

### Local pre-push gates

```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -n0 tests/test_manifest_parser_eight_k.py tests/test_dividend_calendar.py tests/test_dividend_calendar_ingest.py tests/test_manifest_parser_sec_10k.py
uv run pytest    # full suite
```

All four must pass. Pre-push hook enforces the first three;
pytest is the merge gate.

## Coverage parity verification (per `[[legacy-cron-retirement]]`)

The retirement-PR-side parity check runs in PR B, not PR A. PR A
adds dividend_events extraction to the manifest path; PR B then
asserts the manifest path is now coverage-equivalent to the cron
and retires the cron.

PR A's standalone correctness gate:

- **Aristocrats acceptance bar (#434).** ≥80% of Dividend
  Aristocrats fixtures must extract cleanly. The pure-function
  parser is unchanged in this PR, so the existing fixture coverage
  carries forward by construction. The manifest call-site adds NO
  new regex behaviour — it's a pass-through into
  `parse_dividend_announcement`.

- **Hand-picked accession spot-check.** At a minimum, exercise one
  hand-picked 8-K Item 8.01 accession known to land a complete
  (declaration / ex / record / pay / dps) row both via the legacy
  cron pre-PR and via the manifest path post-PR — same row in
  `dividend_events`. Document the chosen accession in the PR body.

- **Sibling fan-out spot-check.** For at least one share-class
  sibling pair (e.g. GOOG/GOOGL), drive the manifest worker on
  one sibling and assert dividend_events rows land for BOTH
  siblings. Document in PR body.

ETL DoD clauses 8-11 (5-instrument smoke + cross-source +
backfill + live-chart verification) are **deferred to the
end-of-epic clean-test pass** per operator direction (2026-05-13)
since the dev DB bootstrap is intentionally broken until the ETL
epic completes. Coverage parity (above) is the standalone per-PR
correctness gate.

## Risks

1. **Dividend extraction regression silently breaks
   `sec_dividend_calendar_ingest` retirement.** Mitigation: PR B
   includes a parity grep on the cron's service-layer calls vs the
   manifest parser's service-layer calls. Both call into
   `upsert_dividend_event` (same function) — coverage parity is
   stable by code-sharing.
2. **`_PARSER_VERSION_DIVIDEND` bump too aggressive on first
   land.** The first composite-version mismatch will cascade-rewash
   every existing 8-K manifest row only when operator manually
   triggers `sec_rebuild`. At ~10 req/s shared rate limit, a full
   8-K backlog drain may take hours. This is operator-controlled,
   not automatic — see "Operator-driven rewash" above. No
   mitigation needed; first-land of #1158 sets the composite
   version with `_PARSER_VERSION_DIVIDEND=1`. Backfill of pre-#1158
   rows requires an explicit operator `sec_rebuild` call after
   merge if the operator wants dividend_events filled for
   already-parsed 8-Ks.
3. **Deterministic dividend-side failure that masks a real 8-K
   bug.** The `try / except / log` pattern logs at exception level
   so operators see the failure in `journalctl`. Per-PR Codex
   pre-push catches the obvious shape; ongoing the
   `app.services.manifest_parsers.eight_k` log channel is the
   surface to watch.
4. **Sibling fan-out on a stale `external_identifiers` row.**
   Mitigated by the fail-closed union pattern from PR #1152 — the
   manifest's anchor `instrument_id` is always in the fan-out set
   even when sibling resolution returns empty/incomplete.
5. **8-K/A amendment fan-out collides with original.** No
   collision: amendments carry distinct accession numbers; upsert
   key includes `source_accession`, so amendments land as new rows.
6. **Backfill ordering of pre-existing 8-K rows.** Sibling
   resolution depends on `external_identifiers` being populated.
   For a CIK whose external_identifiers row is added LATER, a
   manifest re-pending today writes only the anchor instrument_id;
   the sibling addition won't trigger a second write. Operator
   would need to bump `_PARSER_VERSION_DIVIDEND` (or call
   `sec_rebuild` again) to re-fan-out. Acceptable for v1; this is
   the same race every other manifest parser has.

## Out of scope

- Retiring `sec_dividend_calendar_ingest`. That's PR B (a separate
  retirement PR following the same pattern as PRs #1159-#1165).
- Touching `dividend_calendar.ingest_dividend_events`'s tombstone
  / partial-row-TTL logic. PR B drops the cron entirely; if any
  caller of the service module remains after retirement, the
  legacy logic stays intact for them.
- Schema changes to `dividend_events`. The existing UNIQUE key
  `(instrument_id, source_accession)` is already correct.
- LLM-based dividend extraction fallback (#434 phase 2). Out of
  scope.
- Dividend-currency normalisation beyond the existing `currency`
  column default of `'USD'`. Filings outside the US issuer
  population are not in the 8-K manifest path.
- A unified shared fan-out helper across ALL manifest parsers
  (def14a, insider_345, sec_13f_hr, sec_n_port, sec_13dg). PR
  scope is `_siblings.py` shared between sec_10k and eight_k only;
  unifying the rest is its own ticket.

## References

- `[[legacy-cron-retirement]]` memory entry — full coverage diff.
- `[[873-manifest-worker-parser-rollout]]` — manifest-parser
  pattern + `_classify` discriminator + composite-version pattern.
- `[[us-source-coverage]]` — Layer 1/2/3 wiring + retirement
  matrix.
- Issue #1158 — original gap ticket with three options.
- Issue #434 — original dividend_calendar parser ticket +
  Aristocrats acceptance bar.
- Issue #873 — manifest-worker architecture.
- PR #1133 — composite parser_version pattern in 13F-HR.
- PR #1131 — `is_transient_upsert_error` discriminator.
- PR #1152 — share-class fan-out fail-closed pattern in sec_10k.
- Settled-decision §"CIK = entity, CUSIP = security (#1102)" —
  share-class siblings legitimately share CIK.
