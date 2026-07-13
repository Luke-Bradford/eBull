# Persist assembled thesis writer context per run (auditability) — #2017

Split from #2007. Unblocks #2013 / #2010 / #2002.

## Problem

`_assemble_context` (`app/services/thesis.py`) builds the research dict the writer sees, but it is **not persisted**. Root-causing #2007's AMSC defect required rebuilding the context after the fact and *trusting nothing changed* — `fundamentals_snapshot`, `price_daily`, and the `instrument_valuation` view can all move, so no guarantee holds.

Repo rule: *"persist enough structured evidence for auditability"* (settled-decisions §General engineering / Auditability). A thesis memo is a versioned, audited artifact; the evidence it was written from should be **auditable** from storage — specifically its per-block **availability, status, and vintage** — without re-deriving against sources that have since moved.

### Scope of the goal (Codex ckpt-1 HIGH)

This ticket persists **availability + drift-detection metadata**, *not* a faithful byte-reconstruction of the context. `_assemble_context` applies caps, per-block ordering, shaping, a wall-clock news cutoff, and reads mutable views — so the exact writer input is **not** reconstructable from source tables after drift, and a hash + summary does not claim to reconstruct it. Faithful reconstruction would require either the full context blob or per-row identity digests; both are rejected (see Decision) as cost/duplication out of proportion to the #2007-class debugging need. What we capture: *which blocks were present/absent/statused, and as-of when* — enough to audit availability-claim fabrication and to detect that sources moved.

## Source rule

Auditability is a **repo invariant**, not an external data-treatment rule:
- `docs/settled-decisions.md` §Auditability: *"persist structured evidence where it matters; do not leave critical model / recommendation / execution paths unexplained."*
- §Thesis semantics: thesis rows are **append-only** (each generation inserts; never overwrite). `thesis_runs` is the append-only per-attempt record (sql/218).
- §Thesis prompt budget: *"Context-shape changes bump `_PROMPT_VERSION`."* This change does **not** alter context shape (see below) → **no bump**.

No SEC/EDGAR reg governs this; the governing rule is the settled auditability invariant + the append-only `thesis_runs` shape.

## Decision: hash + block-status summary (not full blob)

Persist on `thesis_runs`, forward-only:
- `context_sha256 TEXT` — content-identity fingerprint of the canonically-serialized context.
- `context_summary JSONB` — per-block `{available, status?, as_of?, count?}`.

**Full-blob rejected:** it would faithfully reconstruct the context, but at the cost of duplicating data already in source tables — `prior_thesis.memo_markdown` is a verbatim copy of another `theses` row; `filings[].summary`, `news[]`, and fundamentals all live in their own tables. Settled decision: *"full raw filing text is out of scope for v1."* **Per-row identity digests** (store the PKs + a per-row hash of the rows that fed each block) were also considered and rejected: substantially more machinery for row-level reconstruction that the #2002 judge and #2007-class debugging do not need — they need availability + vintage, not the exact figures. The summary + hash + the existing `thesis_valuation_audit` divergence row (sql/222, band_base vs llm_base) together cover the debugging need.

### What each field proves (be honest)

- **`context_summary`** is the auditability workhorse. Per block it records `available` (usable evidence present vs absent), the **top-level** `status`/`reason`/`quality_status`, and the `as_of` stamp. This directly supports auditing **availability-claim fabrication** (#2007 Defect 2 class): given a stored memo, an auditor/judge (#2002) can check whether the memo asserted data that the block said was `available:false`. The `as_of` stamps are the real **drift-detection** mechanism: "the fundamentals block was as_of 2025-03-31 at write time" — if the latest snapshot is now newer, the sources moved since the memo.
  - **Deliberately not summarized (Codex ckpt-1 MED):** nested per-window statuses (`risk_metrics.windows[].*_status`) and analytics sub-block malformed markers are *not* rolled up into the summary — only the block's top-level availability + status + vintage are. The hash + the source tables cover the finer detail; the summary stays compact and its `status` field is not overclaimed as "every status verbatim."
- **`context_sha256`** is a content-identity fingerprint (two runs with the same hash saw byte-identical context; lets #2013/#2010 tell "regenerated on identical inputs" from "inputs changed"). It **cannot** prove "sources unchanged" by later recomputation: `_assemble_context` is non-reproducible because the news query filters `event_time >= now() - 30d` (wall-clock cutoff moves). This limitation is documented, not worked around — the summary's as-of stamps are the drift signal, not hash recomputation.

## Where to persist: at `_insert_thesis_run`, not on success

`generate_thesis` flow:
1. `_assemble_context` (line 1529) — context in hand.
2. `_insert_thesis_run` (line 1530) — INSERT the 'running' row.
3. `conn.commit()` (line 1542) — **before** the LLM call.
4. LLM writer + critic; on failure → `_record_thesis_run_failure` (status='failed').

Compute `hash_context` + `summarize_context` from the context between steps 1 and 2 and store them in the `_insert_thesis_run` INSERT. The columns therefore commit at step 3, **before** any LLM I/O, and survive every downstream failure path.

**Why not on success (`_finish_thesis_run_ok`):** the #2007 AMSC case was a **guard-rejected** run — the writer emitted an incoherent band, `_validate_writer_output` raised, no `theses` row was stored, and the run row is `status='failed'`. Success-only persistence would have captured **nothing** for exactly the run we needed to debug. Persisting at run-start captures failed/rejected runs too.

Per-thesis lookup on the success path still works via the existing `thesis_runs.thesis_id` FK: `SELECT context_summary FROM thesis_runs WHERE thesis_id = ?`.

## Schema — `sql/223_thesis_runs_context_audit.sql`

New migration file (never edit an applied migration — prevention-log content-drift rule). ALTER the existing table with `IF NOT EXISTS` (prevention-log line 1121; mirrors sql/219's `critic_model` add):

```sql
BEGIN;
ALTER TABLE thesis_runs
    ADD COLUMN IF NOT EXISTS context_sha256  TEXT,
    ADD COLUMN IF NOT EXISTS context_summary JSONB;
COMMIT;
```

Both nullable. Historical rows stay NULL — **no backfill**: past contexts are non-reconstructable (the ticket's whole premise), and reconstructing would re-derive against moved sources. Matches the sql/218/219 nullable-column precedent and the risk_v1 "additive nullable evidence, no version bump, pre-rows stay NULL" blessing.

## Code — new pure module `app/services/thesis_context_audit.py`

Kept out of the already-large `thesis.py`. Pure (no DB, no I/O), fast-tier testable. Takes `prompt_version` as a param so it never imports `thesis._PROMPT_VERSION` (no cycle).

```python
def hash_context(context: Mapping[str, object]) -> str:
    """sha256 of canonically-serialized context (stable key order, compact).

    Strict: NO json default fallback — context is guaranteed JSON-shaped
    (shapers emit isoformat strings + float|None; _to_float kills NaN/inf).
    A non-JSON type is a bug we want to surface, not silently stringify
    (Codex ckpt-1 MED). The fast-tier test proves the strict raise against a
    synthetic non-JSON input; tests/test_thesis.py::
    test_empty_surfaces_yield_honest_absences hashes a REAL assembled
    context, so the helpers are exercised against the true shapes, not just
    hand-built fixtures. The thesis.py call site wraps this so a raise
    degrades to NULL audit columns + a WARNING, never aborts the thesis
    (prevention-log line 2127).
    """
    blob = json.dumps(context, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()

def summarize_context(context: Mapping[str, object], prompt_version: str) -> dict[str, object]:
    return {
        "prompt_version": prompt_version,
        "blocks": {key: _block_status(key, val) for key, val in context.items()},
    }
```

`_block_status(key, val)` — total by construction (never raises; `.get()` only):
- `None` → `{"available": False}`
- `list` → `{"available": bool(val), "count": len(val)}` (+ `as_of` = `max(e[_LIST_ASOF[key]] for e in val)` — **MAX over elements, not `[0]`**: `news` is ordered `importance_score DESC`, so `[0]` is the highest-importance item, not the latest event (Codex ckpt-1 HIGH). ISO-date/timestamp strings sort lexicographically = chronologically, so `max()` is the newest datum the writer saw. The 30d news cutoff is derivable from the run's `started_at`, not stored separately.)
- `dict`:
  - **`risk_metrics`** (special-case, `.get()`-safe): `available` = True, `metric_version` = `val.get("metric_version")`, `as_of` = `max((w.get("as_of_date") for w in val.get("windows") or [] if w.get("as_of_date")), default=None)`. Per-window statuses are *not* rolled up (see "not summarized" above).
  - otherwise:
    - `available` = `bool(val["available"])` if an explicit `available` key exists; else **True iff the dict carries substantive payload** beyond status markers — `bool(set(val) - _MARKER_KEYS)`. This makes a malformed/unsupported analytics wrapper (`{"reason": "malformed"}` / `{"reason": "unsupported_schema", "schema": ...}`) → `available:false` (Codex ckpt-1 MED-2): a status-only dict is *absent usable evidence*, not present.
    - `status` = first present-and-non-None of `("quality_status", "reason")`.
    - `as_of` = `val[_DICT_ASOF[key]]` when present-and-non-None. (`ta_state` has no own stamp — it is derived from the same `price_row` as `price_anchor`, so its vintage is `price_anchor.as_of`; the summary leaves `ta_state` available-only and this sharing is documented here rather than duplicating the stamp.)
- other/scalar → `{"available": val is not None}` (defensive; not expected at top level).

Maps + marker set (explicit, from the shaped-block keys in `_assemble_context`):
```python
_LIST_ASOF = {"fundamentals": "as_of_date", "filings": "filing_date", "news": "event_time"}
_DICT_ASOF = {"prior_thesis": "created_at", "price_anchor": "price_date",
              "valuation": "price_as_of", "fair_value_band": "as_of_date",  # band's own vintage, not the price leg (Codex ckpt-2)
              "analytics_evidence": "as_of"}
_MARKER_KEYS = frozenset({"available", "reason", "status", "quality_status", "schema"})
```

Drift-safe: iterating `context.items()` means a future block always gets at least an `available` entry even if the maps aren't updated. NaN/±inf cannot reach the hash — `_to_float` already maps them to None at assembly (#2007).

### Wiring in `thesis.py`

```python
context = _assemble_context(conn, instrument_id)
# Best-effort audit metadata — must never abort a valid generation
# (prevention-log line 2127; mirrors #2009 divergence "measure-only, never gate").
try:
    context_sha256 = hash_context(context)
    context_summary = summarize_context(context, _PROMPT_VERSION)
except Exception:
    logger.warning("thesis context audit compute failed for instrument_id=%d", instrument_id, exc_info=True)
    context_sha256, context_summary = None, None
run_id = _insert_thesis_run(conn, instrument_id, trigger,
                           provider=..., model=..., critic_model=...,
                           context_sha256=context_sha256, context_summary=context_summary)
```

`_insert_thesis_run` gains two params (both nullable); INSERT gains the two columns (`context_summary` wrapped in `Jsonb` when not None). No change to any other call site — the run insert has a single caller. The defensive wrap means a summarizer/hasher bug degrades to NULL audit columns (visible signal) rather than failing the thesis.

## Not `_PROMPT_VERSION`-bumping

The writer prompt is byte-identical: `_assemble_context` returns the same dict, `_build_writer_prompt` consumes it unchanged. We only *read* that dict to derive audit metadata written to a **different table** (`thesis_runs`, not the writer's input). Settled rule bumps `_PROMPT_VERSION` on context-**shape** changes (what the writer sees); this is not one. `_PROMPT_VERSION` is *recorded inside* `context_summary` (self-describing across future shape changes) — recording ≠ bumping. Consequence: no thesis re-gate, no LLM-eval, no thesis backfill.

## Tests

Fast-tier `tests/test_thesis_context_audit.py`:
- `hash_context` determinism: same dict → same hash; key-order-independent (`sort_keys`); a changed nested value → different hash.
- `summarize_context`:
  - `available:false` for `None` / `{}` / empty-list blocks (never a fabricated present).
  - explicit-`available` blocks (`valuation`, `fair_value_band`) mirror the flag + carry `status`/`as_of`.
  - list blocks carry `count` + **`as_of` = the max dated element, proven with a `news` fixture whose `[0]` is NOT the latest event** (importance-ordered) — guards the Codex HIGH-2 regression.
  - `risk_metrics` → `metric_version` + `as_of` = max window `as_of_date`.
  - malformed analytics (`{"reason": "malformed"}`) → `available:false` + `status:"malformed"` (Codex MED-2).
  - a synthetic unknown block → bare `available` entry (drift-safe fallback); `prompt_version` echoed.

Db-tier `tests/test_thesis_context_audit_persist.py`. Drives the narrow insert/failure seams (`_insert_thesis_run`, `_record_thesis_run_failure`), not full `generate_thesis` — which needs live LLM clients unavailable in the test env (same rationale as `tests/test_thesis_valuation_audit.py`). Since `generate_thesis` calls `_insert_thesis_run` (with the audit) then commits *before* the LLM, and the failure path only UPDATEs `status`, proving the columns are written at insert and untouched by `_record_thesis_run_failure` proves the failed-run capture end-to-end. Cases:
1. **Insert persists:** `_insert_thesis_run(..., context_sha256=…, context_summary=…)` → the row has both columns (JSONB round-trips).
2. **Backward-compat:** omitting the params leaves both columns NULL (historical rows / audit-compute-failure path).
3. **Failed run retains audit (the core requirement, Codex LOW):** after `_record_thesis_run_failure`, the row is `status='failed'` **and still carries non-null `context_sha256` + `context_summary`** — the #2007 AMSC class.

## Out of scope

- Full context-blob storage (rejected above).
- Any FE surface for the summary (no operator UI ticket here; #2002 judge consumes it programmatically).
- Backfill of historical runs (non-reconstructable by design).
- `_PROMPT_VERSION` bump / thesis re-gen.

## Verification (dev)

1. `sql/223` applies at lifespan (smoke test boots against dev DB).
2. Fast + db-tier tests green.
3. After operator restarts the VS Code jobs task (picks up the write path), force-regen one instrument: `POST /instruments/AAPL/thesis?force=true` (Bearer `EBULL_SERVICE_TOKEN`).
4. Confirm the latest `thesis_runs` row for AAPL has non-null `context_sha256` and a `context_summary` with the expected block availabilities (e.g. `fair_value_band.available:true`, `valuation` reflecting its live-quote status).

## Codex

Codex ckpt-1 reviews this spec before operator sign-off (correctness gaps, invariant violations, missing edge cases; source-rule citation check).
