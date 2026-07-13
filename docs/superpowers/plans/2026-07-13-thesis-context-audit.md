# Thesis Context Audit (#2017) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Persist a content hash + per-block availability/status/as-of summary of the assembled thesis writer context onto `thesis_runs`, at run-insert time, so a memo's evidence is auditable from storage (incl. failed/guard-rejected runs).

**Architecture:** New pure module (`thesis_context_audit.py`) computes `hash_context` + `summarize_context` from the context dict `_assemble_context` already builds. `generate_thesis` computes both right after assembly (defensively wrapped) and passes them into `_insert_thesis_run`, which writes two new nullable columns on `thesis_runs`. The insert commits before the LLM call, so the audit survives every downstream failure path.

**Tech Stack:** Python 3.14, psycopg3, Postgres, pytest (fast tier + `db` tier).

## Global Constraints

- **No `_PROMPT_VERSION` bump.** Writer prompt is byte-identical; this only reads the context to write audit metadata to a different table. `_PROMPT_VERSION` (`app/services/thesis.py:124`, currently `"v4"`) is *recorded inside* the summary, not incremented.
- **No backfill.** Both columns nullable; historical `thesis_runs` rows stay NULL (past contexts are non-reconstructable). Matches the sql/218/219 nullable-column precedent.
- **Best-effort, never gate.** Audit compute must never abort a valid thesis (prevention-log line 2127; mirrors #2009 divergence "measure-only"). Defensive wrap at the call site → NULL columns + WARNING log on failure.
- **Migration discipline.** New file `sql/223_*.sql` (never edit an applied migration — prevention-log content-drift rule). `ADD COLUMN IF NOT EXISTS` (prevention-log line 1121; mirrors sql/219's `critic_model` add).
- **Pure module is `.get()`-only / total** — `_block_status` never raises on any block shape.
- Spec: `docs/superpowers/specs/2026-07-13-thesis-context-audit-design.md`.

---

## File Structure

- Create `sql/223_thesis_runs_context_audit.sql` — the two nullable columns on `thesis_runs`.
- Create `app/services/thesis_context_audit.py` — pure `hash_context` + `summarize_context` + `_block_status` + the as-of/marker maps.
- Create `tests/test_thesis_context_audit.py` — fast-tier tests for the pure module.
- Create `tests/test_thesis_context_audit_persist.py` — db-tier: the two columns persist on insert and survive a failed run.
- Modify `app/services/thesis.py` — import the two functions; extend `_insert_thesis_run` (signature + INSERT); compute + pass in `generate_thesis`.

---

### Task 1: Migration — two nullable columns on `thesis_runs`

**Files:**
- Create: `sql/223_thesis_runs_context_audit.sql`

**Interfaces:**
- Consumes: existing `thesis_runs` table (sql/218).
- Produces: `thesis_runs.context_sha256 TEXT`, `thesis_runs.context_summary JSONB` (both nullable) — consumed by Task 3.

- [ ] **Step 1: Write the migration**

```sql
-- 223: persist assembled thesis writer context per run (#2017)
--
-- Spec: docs/superpowers/specs/2026-07-13-thesis-context-audit-design.md
--
-- Altered tables:
--   thesis_runs — context_sha256 (content-identity fingerprint of the
--                 assembled writer context) + context_summary (per-block
--                 availability/status/as-of JSONB). Both nullable; written
--                 at run-insert BEFORE the LLM call so failed/guard-rejected
--                 runs are captured too (the #2007 AMSC debugging class).
--                 Historical rows stay NULL — past contexts are
--                 non-reconstructable, so there is no backfill.
--
-- Not a context-SHAPE change (the writer prompt is byte-identical), so
-- _PROMPT_VERSION is NOT bumped; the version is recorded inside the summary.

BEGIN;

ALTER TABLE thesis_runs
    ADD COLUMN IF NOT EXISTS context_sha256  TEXT,
    ADD COLUMN IF NOT EXISTS context_summary JSONB;

COMMIT;
```

- [ ] **Step 2: Apply migrations and verify the columns exist**

`run_migrations()` (`app/db/migrations.py:115`) takes no args — it reads the configured DSN and applies every `sql/NNN_*.sql` in order. Apply against the dev DB and verify:

```bash
uv run python -c "from app.db.migrations import run_migrations; print(run_migrations()[-3:])"
uv run python - <<'PY'
from app.db.pool import open_pool
with open_pool() as pool, pool.connection() as c:
    cols = c.execute("""
        SELECT column_name, data_type, is_nullable FROM information_schema.columns
        WHERE table_name='thesis_runs' AND column_name IN ('context_sha256','context_summary')
        ORDER BY column_name
    """).fetchall()
    print(cols)
PY
```
Expected: the migration list ends with `223_thesis_runs_context_audit.sql`, and the columns print as `[('context_sha256','text','YES'), ('context_summary','jsonb','YES')]`.

> The db-tier tests (Task 3) apply sql/223 automatically: the `ebull_test_db` fixture rebuilds its template DB whenever `_migration_hash()` changes (`tests/fixtures/ebull_test_db.py`), and adding sql/223 changes that hash. No manual test-DB apply needed — this step is the dev-DB verification path.

> If `open_pool()` is not the right dev entrypoint here, fall back to the smoke test (`uv run pytest tests/smoke`), which boots the FastAPI lifespan and runs migrations against dev — a green smoke run proves sql/223 applies cleanly.

- [ ] **Step 3: Commit**

```bash
git add sql/223_thesis_runs_context_audit.sql
git commit -m "feat(#2017): sql/223 — thesis_runs context-audit columns"
```

---

### Task 2: Pure module — `hash_context` + `summarize_context`

**Files:**
- Create: `app/services/thesis_context_audit.py`
- Test: `tests/test_thesis_context_audit.py`

**Interfaces:**
- Consumes: nothing (pure; takes `prompt_version` as a param so it never imports `thesis._PROMPT_VERSION`).
- Produces:
  - `hash_context(context: Mapping[str, object]) -> str`
  - `summarize_context(context: Mapping[str, object], prompt_version: str) -> dict[str, object]`
  Both consumed by Task 3.

- [ ] **Step 1: Write the failing tests**

```python
# tests/test_thesis_context_audit.py
"""Fast-tier tests for the pure thesis context-audit helpers (#2017).

No DB: hash determinism + per-block availability/status/as-of summary.
"""

from __future__ import annotations

from app.services.thesis_context_audit import hash_context, summarize_context

_PV = "v4"


def _full_context() -> dict[str, object]:
    """A representative _assemble_context-shaped dict (subset of real fields)."""
    return {
        "instrument": {"symbol": "AAPL", "company_name": "Apple", "currency": "USD"},
        "fundamentals": [
            {"as_of_date": "2025-03-31", "revenue_ttm": 4.0e11},
            {"as_of_date": "2024-12-31", "revenue_ttm": 3.9e11},
        ],
        "filings": [{"filing_date": "2025-05-01", "filing_type": "10-Q", "summary": "…"}],
        # news is ordered importance DESC, NOT recency — [0] is NOT the newest event.
        "news": [
            {"event_time": "2025-07-01T00:00:00+00:00", "headline": "high-importance older"},
            {"event_time": "2025-07-10T00:00:00+00:00", "headline": "low-importance newer"},
        ],
        "prior_thesis": {"version": 3, "stance": "buy", "created_at": "2025-07-01T12:00:00+00:00"},
        "risk_metrics": {
            "metric_version": "risk_v1",
            "windows": [
                {"window_key": "1y", "as_of_date": "2025-07-11", "cagr_status": "ok"},
                {"window_key": "3y", "as_of_date": "2025-07-11", "cagr_status": "thin_history"},
            ],
        },
        "price_anchor": {"close": 210.0, "price_date": "2025-07-11", "currency": "USD"},
        "valuation": {"available": True, "current_price": 210.0, "price_as_of": "2025-07-11"},
        # representative _shape_fair_value_band output — as_of_date (band vintage)
        # deliberately DISTINCT from price_as_of so the test proves we pick as_of_date.
        "fair_value_band": {
            "available": True, "reason": None, "quality_status": "ok",
            "bear": 150.0, "base": 200.0, "bull": 250.0,
            "as_of_date": "2025-06-30", "ttm_end": "2025-03-31", "price_as_of": "2025-07-11",
            "basis": {},
        },
        "analytics_evidence": {"schema": "iar_v1", "as_of": "2025-07-09T00:00:00+00:00", "piotroski": {"score": 7}},
        "ta_state": {"sma_50": 205.0, "sma_200": 190.0, "price_vs_sma200": "above"},
        "earnings_history": [],
        "analyst_estimates": None,
    }


# --- hash_context ---------------------------------------------------------

def test_hash_is_deterministic_and_key_order_independent() -> None:
    a = {"x": 1, "y": {"b": 2, "a": 3}, "z": [1, 2]}
    b = {"z": [1, 2], "y": {"a": 3, "b": 2}, "x": 1}  # same content, keys reordered
    assert hash_context(a) == hash_context(b)


def test_hash_changes_on_nested_value_change() -> None:
    a = _full_context()
    b = _full_context()
    b["price_anchor"] = {**a["price_anchor"], "close": 999.0}  # type: ignore[dict-item]
    assert hash_context(a) != hash_context(b)


def test_hash_is_64_hex_chars() -> None:
    h = hash_context(_full_context())
    assert len(h) == 64 and all(c in "0123456789abcdef" for c in h)


# --- summarize_context ----------------------------------------------------

def test_prompt_version_echoed() -> None:
    s = summarize_context(_full_context(), _PV)
    assert s["prompt_version"] == _PV


def test_absent_blocks_marked_unavailable_never_fabricated() -> None:
    ctx = {"analyst_estimates": None, "earnings_history": [], "empty_dict": {}}
    blocks = summarize_context(ctx, _PV)["blocks"]
    assert blocks["analyst_estimates"] == {"available": False}
    assert blocks["earnings_history"] == {"available": False, "count": 0}
    assert blocks["empty_dict"] == {"available": False}


def test_list_as_of_is_max_not_first_element() -> None:
    # Guards Codex HIGH-2: news[0] is the high-importance OLDER event; the
    # summary as_of must be the newest event_time (max), 2025-07-10.
    blocks = summarize_context(_full_context(), _PV)["blocks"]
    assert blocks["news"] == {"available": True, "count": 2, "as_of": "2025-07-10T00:00:00+00:00"}
    assert blocks["fundamentals"]["as_of"] == "2025-03-31"  # DESC-ordered, max = latest


def test_explicit_available_blocks_mirror_flag_and_carry_status_asof() -> None:
    blocks = summarize_context(_full_context(), _PV)["blocks"]
    # fair_value_band as_of = the band's own as_of_date (2025-06-30), NOT price_as_of.
    assert blocks["fair_value_band"] == {"available": True, "status": "ok", "as_of": "2025-06-30"}
    assert blocks["valuation"] == {"available": True, "as_of": "2025-07-11"}  # present → no status field


def test_valuation_absent_carries_reason() -> None:
    ctx = {"valuation": {"available": False, "reason": "no_live_quote"}}
    assert summarize_context(ctx, _PV)["blocks"]["valuation"] == {
        "available": False,
        "status": "no_live_quote",
    }


def test_malformed_analytics_is_unavailable() -> None:
    # Codex MED-2: a status-only dict is absent usable evidence, not present.
    ctx = {"analytics_evidence": {"reason": "malformed"}}
    assert summarize_context(ctx, _PV)["blocks"]["analytics_evidence"] == {
        "available": False,
        "status": "malformed",
    }


def test_unsupported_schema_analytics_is_unavailable() -> None:
    ctx = {"analytics_evidence": {"reason": "unsupported_schema", "schema": "iar_v2"}}
    assert summarize_context(ctx, _PV)["blocks"]["analytics_evidence"] == {
        "available": False,
        "status": "unsupported_schema",
    }


def test_risk_metrics_carries_version_and_max_window_asof() -> None:
    blocks = summarize_context(_full_context(), _PV)["blocks"]
    assert blocks["risk_metrics"] == {
        "available": True,
        "metric_version": "risk_v1",
        "as_of": "2025-07-11",
    }


def test_ta_state_and_instrument_available_only() -> None:
    blocks = summarize_context(_full_context(), _PV)["blocks"]
    assert blocks["ta_state"] == {"available": True}
    assert blocks["instrument"] == {"available": True}
    assert blocks["price_anchor"] == {"available": True, "as_of": "2025-07-11"}
    assert blocks["prior_thesis"] == {"available": True, "as_of": "2025-07-01T12:00:00+00:00"}


def test_unknown_block_gets_drift_safe_available_entry() -> None:
    ctx = {"a_future_block": {"some": "payload"}}
    assert summarize_context(ctx, _PV)["blocks"]["a_future_block"] == {"available": True}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_thesis_context_audit.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.services.thesis_context_audit'`.

- [ ] **Step 3: Write the module**

```python
# app/services/thesis_context_audit.py
"""Pure helpers persisting what the thesis writer saw, per run (#2017).

`_assemble_context` (:mod:`app.services.thesis`) builds the writer's research
dict but does not persist it. These helpers derive compact, auditable
metadata from that dict — a content hash and a per-block
availability/status/as-of summary — stored on ``thesis_runs`` at run-insert.
Enough to audit availability-claim fabrication (#2007 Defect 2 class) and to
detect that sources moved, WITHOUT duplicating the source rows.

Pure: no DB, no I/O. ``prompt_version`` is a parameter (not an import) so this
module never depends on ``thesis`` — no import cycle.
"""

from __future__ import annotations

import hashlib
import json
from collections.abc import Mapping

# As-of field name per shaped context block (see thesis._assemble_context).
# List blocks are date-carrying element lists; the summary reports max(stamp).
_LIST_ASOF: dict[str, str] = {
    "fundamentals": "as_of_date",
    "filings": "filing_date",
    "news": "event_time",
}
_DICT_ASOF: dict[str, str] = {
    "prior_thesis": "created_at",
    "price_anchor": "price_date",
    "valuation": "price_as_of",
    # the band's OWN vintage (fair_value_band_current.as_of_date), NOT the
    # price leg (price_as_of) — the band is fundamentals-anchored (Codex ckpt-2).
    "fair_value_band": "as_of_date",
    "analytics_evidence": "as_of",
}
# A dict whose keys are ALL markers (no substantive payload) is "absent usable
# evidence", not present — e.g. a malformed/unsupported analytics wrapper.
_MARKER_KEYS: frozenset[str] = frozenset({"available", "reason", "status", "quality_status", "schema"})


def hash_context(context: Mapping[str, object]) -> str:
    """sha256 of the canonically-serialized context (stable key order, compact).

    Strict — no json ``default`` fallback. The thesis context is guaranteed
    JSON-shaped (shapers emit isoformat strings + float|None; ``_to_float``
    maps NaN/inf to None), so a non-serializable type is a bug to surface, not
    silently stringify. The db-tier test hashes a real assembled context, and
    the caller wraps this defensively so a raise degrades to NULL audit columns
    rather than aborting a thesis.

    Note: this fingerprints the exact context bytes; it does NOT prove
    "sources unchanged" by later recomputation (``_assemble_context`` is
    non-reproducible — the news query uses a wall-clock 30d cutoff). Drift is
    detected via the summary's as-of stamps, not by re-hashing.
    """
    blob = json.dumps(context, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(blob.encode("utf-8")).hexdigest()


def summarize_context(context: Mapping[str, object], prompt_version: str) -> dict[str, object]:
    """Per-block availability/status/as-of summary of the writer context.

    ``prompt_version`` is recorded (self-describing across future context-shape
    changes) — recording is not a ``_PROMPT_VERSION`` bump.
    """
    return {
        "prompt_version": prompt_version,
        "blocks": {key: _block_status(key, val) for key, val in context.items()},
    }


def _block_status(key: str, val: object) -> dict[str, object]:
    """Availability (+ optional status/as-of/count) for one context block.

    Total by construction: ``.get()`` only, never raises, so the summary is
    safe over any block shape. A block absent from the maps still gets an
    ``available`` entry (drift-safe).
    """
    if val is None:
        return {"available": False}

    if isinstance(val, list):
        out: dict[str, object] = {"available": bool(val), "count": len(val)}
        asof_key = _LIST_ASOF.get(key)
        if asof_key is not None:
            stamps = [
                e.get(asof_key) for e in val if isinstance(e, Mapping) and e.get(asof_key) is not None
            ]
            if stamps:
                # ISO date/timestamp strings sort lexicographically = chronologically.
                out["as_of"] = max(stamps)
        return out

    if isinstance(val, Mapping):
        if key == "risk_metrics":
            windows = val.get("windows") or []
            stamps = [
                w.get("as_of_date")
                for w in windows
                if isinstance(w, Mapping) and w.get("as_of_date") is not None
            ]
            out = {"available": True, "metric_version": val.get("metric_version")}
            if stamps:
                out["as_of"] = max(stamps)
            return out

        if "available" in val:
            available = bool(val["available"])
        else:
            # present iff it carries payload beyond status markers
            available = bool(set(val) - _MARKER_KEYS)
        out = {"available": available}
        for status_key in ("quality_status", "reason"):
            status = val.get(status_key)
            if status is not None:
                out["status"] = status
                break
        asof_key = _DICT_ASOF.get(key)
        if asof_key is not None:
            asof = val.get(asof_key)
            if asof is not None:
                out["as_of"] = asof
        return out

    # scalar / unexpected top-level type — defensive (not expected).
    return {"available": val is not None}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `uv run pytest tests/test_thesis_context_audit.py -q`
Expected: PASS (all cases).

- [ ] **Step 5: Lint/type/format the new files**

Run:
```bash
uv run ruff check app/services/thesis_context_audit.py tests/test_thesis_context_audit.py
uv run ruff format --check app/services/thesis_context_audit.py tests/test_thesis_context_audit.py
uv run pyright app/services/thesis_context_audit.py
```
Expected: clean.

- [ ] **Step 6: Commit**

```bash
git add app/services/thesis_context_audit.py tests/test_thesis_context_audit.py
git commit -m "feat(#2017): pure thesis context-audit helpers (hash + block summary)"
```

---

### Task 3: Wire into `thesis.py` + db-tier persist tests

**Files:**
- Modify: `app/services/thesis.py` (import ~line 74; `_insert_thesis_run` at `app/services/thesis.py:1395`; `generate_thesis` at `app/services/thesis.py:1529`)
- Test: `tests/test_thesis_context_audit_persist.py`

**Interfaces:**
- Consumes: `hash_context`, `summarize_context` (Task 2); `context_sha256`/`context_summary` columns (Task 1).
- Produces: `_insert_thesis_run(..., context_sha256=None, context_summary=None)` — two new keyword-only params, both defaulting to None (existing single caller + the precedent tests stay valid).

- [ ] **Step 1: Write the failing db-tier tests**

```python
# tests/test_thesis_context_audit_persist.py
"""DB-tier proof that the #2017 context-audit columns persist on the run row
and survive a failed run.

Drives the narrow insert/failure seams (`_insert_thesis_run`,
`_record_thesis_run_failure`) rather than the full `generate_thesis` path,
which needs live LLM clients unavailable in the test env — same rationale as
tests/test_thesis_valuation_audit.py. `generate_thesis` calls
`_insert_thesis_run` (with the context audit) then commits BEFORE the LLM, and
the failure path only UPDATEs status — so proving the columns are written at
insert and untouched by `_record_thesis_run_failure` proves the failed-run
capture end to end.
"""

from __future__ import annotations

import pytest

from app.services.thesis import _insert_thesis_run, _record_thesis_run_failure

pytestmark = pytest.mark.db

_SUMMARY = {
    "prompt_version": "v4",
    "blocks": {
        "fundamentals": {"available": True, "count": 5, "as_of": "2025-03-31"},
        "valuation": {"available": False, "status": "no_live_quote"},
    },
}


@pytest.fixture
def conn(ebull_test_conn):
    return ebull_test_conn


def _seed_instrument(conn, instrument_id: int) -> int:
    conn.execute(
        "INSERT INTO instruments (instrument_id, symbol, company_name, is_tradable) VALUES (%s, %s, %s, TRUE)",
        (instrument_id, "TCA", "Thesis Context Audit Test Co"),
    )
    conn.commit()
    return instrument_id


def test_insert_persists_context_audit(conn) -> None:
    iid = _seed_instrument(conn, 9151)
    run_id = _insert_thesis_run(
        conn, iid, "manual",
        provider="openai_compatible", model="qwen3:14b", critic_model="qwen3:14b",
        context_sha256="a" * 64, context_summary=_SUMMARY,
    )
    conn.commit()

    row = conn.execute(
        "SELECT context_sha256, context_summary FROM thesis_runs WHERE run_id = %s", (run_id,)
    ).fetchone()
    assert row[0] == "a" * 64
    assert row[1] == _SUMMARY  # JSONB round-trips to the same dict


def test_missing_context_audit_leaves_nulls(conn) -> None:
    # Backward-compat: existing callers (and historical rows) leave both NULL.
    iid = _seed_instrument(conn, 9152)
    run_id = _insert_thesis_run(
        conn, iid, "manual", provider="openai_compatible", model="qwen3:14b", critic_model="qwen3:14b"
    )
    conn.commit()
    row = conn.execute(
        "SELECT context_sha256, context_summary FROM thesis_runs WHERE run_id = %s", (run_id,)
    ).fetchone()
    assert row == (None, None)


def test_failed_run_retains_context_audit(conn) -> None:
    # The #2017 core property: audit written at insert (before the LLM)
    # survives the failure path (status-only UPDATE) — the #2007 AMSC class.
    iid = _seed_instrument(conn, 9153)
    run_id = _insert_thesis_run(
        conn, iid, "scheduled",
        provider="openai_compatible", model="qwen3:14b", critic_model="qwen3:14b",
        context_sha256="b" * 64, context_summary=_SUMMARY,
    )
    conn.commit()

    _record_thesis_run_failure(conn, run_id, ValueError("Writer: incoherent targets bear>base"))

    row = conn.execute(
        "SELECT status, context_sha256, context_summary FROM thesis_runs WHERE run_id = %s", (run_id,)
    ).fetchone()
    assert row[0] == "failed"
    assert row[1] == "b" * 64
    assert row[2] == _SUMMARY
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `uv run pytest tests/test_thesis_context_audit_persist.py -q`
Expected: FAIL — `_insert_thesis_run() got an unexpected keyword argument 'context_sha256'` (and/or `UndefinedColumn` if sql/223 already applied but the params aren't threaded).

- [ ] **Step 3: Add the import in `thesis.py`**

At `app/services/thesis.py`, in the `app.services` import group (after line 75, `from app.services.technical_analysis import derive_trend_signals`), add:

```python
from app.services.thesis_context_audit import hash_context, summarize_context
```

- [ ] **Step 4: Extend `_insert_thesis_run` (signature + INSERT)**

Replace the function body at `app/services/thesis.py:1395` with the two new params threaded into the INSERT:

```python
def _insert_thesis_run(
    conn: psycopg.Connection[Any],
    instrument_id: int,
    trigger: RunTrigger,
    *,
    provider: str,
    model: str,
    critic_model: str,
    context_sha256: str | None = None,
    context_summary: dict[str, object] | None = None,
) -> int:
    """Insert a 'running' thesis_runs row and return its run_id.

    ``model`` (writer) and ``critic_model`` are the CONFIGURED models
    (the run may fail before any provider response exists); the stored
    thesis row carries the writer model as reported by the response, and
    ``critic_json.model`` the critic's. Recording ``critic_model`` here
    is what keeps critic provenance auditable when the best-effort critic
    fails and no ``critic_json`` is stored (#1995).

    ``context_sha256`` / ``context_summary`` (#2017) fingerprint + summarize
    the assembled writer context. Written HERE — before the LLM call and the
    pre-LLM commit — so failed/guard-rejected runs retain the audit. Both
    nullable: a caller that omits them (or an audit-compute failure upstream)
    leaves the columns NULL.
    """
    row = conn.execute(
        """
        INSERT INTO thesis_runs (instrument_id, trigger, provider, model, critic_model,
                                 context_sha256, context_summary)
        VALUES (%(instrument_id)s, %(trigger)s, %(provider)s, %(model)s, %(critic_model)s,
                %(context_sha256)s, %(context_summary)s)
        RETURNING run_id
        """,
        {
            "instrument_id": instrument_id,
            "trigger": trigger,
            "provider": provider,
            "model": model,
            "critic_model": critic_model,
            "context_sha256": context_sha256,
            "context_summary": Jsonb(context_summary) if context_summary is not None else None,
        },
    ).fetchone()
    if row is None:
        raise RuntimeError(f"INSERT INTO thesis_runs did not RETURN a row for instrument_id={instrument_id}")
    return int(row[0])
```

- [ ] **Step 5: Compute + pass in `generate_thesis`**

At `app/services/thesis.py:1529`, replace:

```python
    context = _assemble_context(conn, instrument_id)
    run_id = _insert_thesis_run(
        conn,
        instrument_id,
        trigger,
        provider=clients.writer.provider_name,
        model=clients.writer.model,
        critic_model=clients.critic.model,
    )
```

with:

```python
    context = _assemble_context(conn, instrument_id)
    # #2017: fingerprint + summarize what the writer saw, persisted on the run
    # row. Best-effort — audit compute must NEVER abort a valid generation
    # (prevention-log line 2127; mirrors #2009 divergence "measure-only, never
    # gate"). A failure degrades to NULL audit columns + a WARNING.
    try:
        context_sha256: str | None = hash_context(context)
        context_summary: dict[str, object] | None = summarize_context(context, _PROMPT_VERSION)
    except Exception:
        logger.warning(
            "thesis context audit compute failed for instrument_id=%d", instrument_id, exc_info=True
        )
        context_sha256, context_summary = None, None
    run_id = _insert_thesis_run(
        conn,
        instrument_id,
        trigger,
        provider=clients.writer.provider_name,
        model=clients.writer.model,
        critic_model=clients.critic.model,
        context_sha256=context_sha256,
        context_summary=context_summary,
    )
```

- [ ] **Step 6: Run the db-tier tests to verify they pass**

Run:
```bash
docker compose --profile test up -d postgres-test   # if not already up
uv run pytest tests/test_thesis_context_audit_persist.py -q
```
Expected: PASS (3 tests). This run applies sql/223 if the test harness runs migrations on connect; if the columns are missing, apply sql/223 first (Task 1 Step 2).

- [ ] **Step 7: Guard against regressions in the existing thesis_runs suite**

Run: `uv run pytest tests/test_thesis_runs_db.py -q`
Expected: PASS — the new params default to None, so the precedent lifecycle/failure tests are unchanged.

- [ ] **Step 8: Full pre-push gate**

Run:
```bash
uv run ruff check .
uv run ruff format --check .
uv run pyright
uv run pytest -m "not db"
uv run pytest tests/smoke
uv run pytest -m db -k "thesis"
```
Expected: all clean. (`tests/smoke` boots the FastAPI lifespan against dev — proves sql/223 applies cleanly. The `-m db -k thesis` run exercises the new persist tests + the thesis_runs/valuation-audit precedents together.)

- [ ] **Step 9: Commit**

```bash
git add app/services/thesis.py tests/test_thesis_context_audit_persist.py
git commit -m "feat(#2017): persist context hash + block summary on thesis_runs at run-insert"
```

---

## Self-Review (completed at authoring)

- **Spec coverage:** schema (Task 1) ✓; hash + summary pure module w/ all Codex-hardened rules — news max(), risk_metrics special-case, malformed→unavailable, strict hash (Task 2) ✓; persist-at-run-insert wiring + defensive wrap + no-bump (Task 3) ✓; fast-tier + db-tier incl. failed-run capture (Tasks 2/3) ✓; no backfill / nullable (Task 1 + Global Constraints) ✓.
- **Placeholder scan:** none — every code step carries full code; the only ellipses are inside test-fixture string values (`"summary": "…"`), not plan steps.
- **Type consistency:** `hash_context`/`summarize_context` signatures identical across Tasks 2↔3; `_insert_thesis_run` new params (`context_sha256: str | None`, `context_summary: dict[str, object] | None`) match the call site and the tests; `Jsonb` already imported in thesis.py (`app/services/thesis.py:56`).
- **Deviation from spec (noted):** db-tier failed-run proof uses the `_insert_thesis_run` + `_record_thesis_run_failure` seams instead of a faked `LLMClientPair` (full `generate_thesis` needs live LLM — same rationale as the sql/222 test). Same property proven; the spec test section is updated to match.
