# Thesis engine goes live — BYO OpenAI-compatible LLM provider (#1919)

**Status:** spec (pre-implementation). **Issue:** #1919. **Endpoint handoff:** #1888 (closed).
**Local-first mandate (operator, 2026-07-09):** default provider = operator-local OpenAI-compatible
endpoint; cloud (Anthropic/OpenAI) remains available by configuration only. Heavy testing happens on
the local model where tokens are free.

## Source rule

No SEC reg governs this change; the governing contracts are internal:

- **#1888 endpoint contract:** BYO-LLM — eBull plugs into an OpenAI-compatible base URL
  (`http://localhost:11434/v1`, Ollama; `GET /v1/models` lists models; key ignored). The LLM is
  external/configurable, never shipped in-repo.
- **settled-decisions "Thesis semantics"** (docs/settled-decisions.md:143-185): versioning-by-insert,
  never overwrite; `critic_json` separate from `memo_markdown`; thesis_type ∈ {compounder, value,
  turnaround, speculative}; stance ∈ {buy, hold, watch, avoid}; freshness = latest `created_at` vs
  `coverage.review_frequency` (daily=1/weekly=7/monthly=30); **capped prompt budget** (1 prior thesis,
  3 filing events, 5 fundamentals snapshots, 10 news items/30d) — PLUS the separately-settled
  risk-evidence block (#1632: `instrument_risk_metrics_current` scalars,
  docs/settled-decisions.md:230 + docs/specs/thesis/2026-06-18-risk-evidence-ingestion.md), already
  live in `_assemble_context` (thesis.py:511-527). This spec changes NONE of these. Further context
  enrichment is out of scope (#1987 — design-first + settled-decision amendment).
- **#293:** `generate_thesis` commits the read transaction BEFORE the LLM call. Preserved — the
  provider layer changes only what happens inside `_call_writer`/`_call_critic`.
- **#273:** filing-event supersede triggers (10-K/10-Q/8-K + /A variants, `thesis.py:232-234`,
  ingest-time comparison). Preserved unchanged.
- **#1479 class (bounded outbound I/O):** every outbound LLM call must go through a single
  construction chokepoint with bounded httpx timeouts, lint-enforced
  (`scripts/check_anthropic_timeout.sh` precedent).

## Current state (verified 2026-07-09, file:line)

The engine is complete but generates nothing:

- `app/services/thesis.py` (906 lines): writer+critic, JSON schema validation, versioned inserts.
  Hardcoded `anthropic.Anthropic` (`:40`), `_MODEL="claude-sonnet-4-6"` (`:76`),
  `_MAX_TOKENS_WRITER=2048` / `_MAX_TOKENS_CRITIC=1024` (`:77-78`).
- Dev `theses` table: **0 rows** (no key configured; every path gated on `anthropic_api_key`).
- `daily_thesis_refresh` (`app/workers/scheduler.py:3068`) is **dormant**: absent from
  `SCHEDULED_JOBS` and `_INVOKERS`, zero `job_runs` rows ever;
  `tests/test_workers_scheduler_registry.py:52-61` pins it INTERNAL_ONLY.
  `docs/wiki/job-registry-audit.md:349` falsely claims it is invokable — fix in this work.
  Consequence: age-based staleness never fires; only the filing cascade
  (`daily_financial_facts` Phase 3, `scheduler.py:2948-2993`) and the manual POST generate theses.
- `POST /instruments/{symbol}/thesis` (`app/api/theses.py:277`): hard 24h cache, **no force
  parameter**; reads `os.environ.get("ANTHROPIC_API_KEY")` directly (`:47`) instead of `settings`.
- **No auth**: `theses_router` + `instrument_thesis_router` are included bare
  (`app/main.py:562-563`) — the LLM-spending POST and all memo reads are unauthenticated.
- Failures: writer bad-JSON raises with no retry (`thesis.py:652-674`); critic swallows everything →
  `{}` (`:717-741`); job-path failures land in logs only (`scheduler.py:3156-3163`). Nothing for
  #1902/#1901 to render.
- `theses` rows record **neither model nor provider nor prompt version** (sql/001_init.sql:87-104 +
  sql/006 critic_json only) — violates "version model outputs where required".
- All 4 LLM call sites are Anthropic-typed (SDK exceptions, `message.content[0].text`);
  `make_anthropic_client` (`app/services/anthropic_client.py:65`) is a bounded-timeout factory
  (read=180s) whose construction site is lint-enforced.

## Empirical verification (2026-07-09, local endpoint)

- `GET http://localhost:11434/v1/models` → `qwen3:14b` (9.3 GB) + `deepseek-r1:14b` (9.0 GB) live.
- **qwen3:14b default mode FAILS the JSON contract**: thinking burned the whole 400-token budget
  (`finish_reason: length`, empty content). With `/no_think` in the system prompt +
  `response_format: {"type":"json_object"}`: valid schema-conformant JSON, `finish: stop`,
  91 completion tokens. Two hard requirements fall out: (a) disable/strip thinking, (b) always check
  `finish_reason` to distinguish truncation from malformed output.
- Volume (dev DB): 12,603 tradable instruments; coverage T1=5, T2=669, T3=11,933; held=5.
  Per thesis ≈ 4-6k input + ≤3,072 output tokens (2 calls). Held+top-20 ≈ 50 calls first run,
  ~1-2/day steady-state. Naive T1+T2 enablement = 674 theses (1,348 calls) ≈ 17-40h serial on a
  local 14B — batch bounds are mandatory.

## Design

### 1. Provider layer — `app/services/llm_client.py`

New module, thesis-scoped (sentiment stays on Anthropic + lexicon fallback; out of scope).

```python
class LLMCompletion:      # normalized result
    text: str             # <think>…</think> stripped defensively
    finish_reason: str    # "stop" | "length" | provider-mapped
    model: str            # as reported by the provider response
class LLMClient(Protocol):
    def complete(self, *, system: str, user: str, max_tokens: int) -> LLMCompletion: ...
```

- `OpenAICompatProvider`: `httpx.post(f"{base_url}/chat/completions", ...)` with
  `response_format={"type":"json_object"}`. **No new dependency** — the `openai` package is not
  added; httpx is already in the tree. Timeouts: connect 5s / read 600s / write 30s / pool 10s
  (a 14B emitting 2,048 tokens below 11.4 tok/s breaks a 180s read window — 600s bounds the #1479
  hang class without killing slow local decodes).
- `AnthropicProvider`: wraps the existing `make_anthropic_client` (unchanged 180s read).
- Single construction site `make_llm_client(conn)` resolving from config (§2).
  **Lint:** extend the chokepoint script (sibling of `check_anthropic_timeout.sh`) so
  `httpx` LLM construction outside `llm_client.py` fails the gate.
- **Concurrency:** two layers, honest about the settled process topology (#719,
  docs/settled-decisions.md:372 — API and jobs daemon are SEPARATE processes, so no in-process
  primitive can serialise across them). (a) Per-process `threading.Semaphore(1)` around
  `complete()` stops one process stacking its own concurrent calls. (b) Cross-process contention
  resolves at the **Ollama server-side request queue** (serial by default): a manual POST landing
  during a scheduled generation queues, and the 600s read window is sized to survive queue depth
  2–3 at 14B speeds. No DB advisory lock around LLM calls — holding pool resources through
  multi-minute generations is the failure class #293 removed.
- `_call_writer`/`_call_critic` switch to `LLMClient` and gain: strip `<think>` block before JSON
  parse; on schema `ValueError` retry **once**; record `finish_reason` so truncation is
  distinguishable from malformed output in the failure record.

### 2. Configuration — knobs in `runtime_config`, key in env

Precedent: `display_currency` (sql/023_live_pricing_currency.sql) — operator-editable singleton
columns, audited via `runtime_config_audit` (extend its field CHECK, `:55`), surfaced on
`GET/PATCH /config` + SettingsPage.

- New columns: `llm_provider TEXT NOT NULL DEFAULT 'openai_compatible'`
  (CHECK IN ('openai_compatible','anthropic')), `llm_base_url TEXT NOT NULL DEFAULT
  'http://localhost:11434/v1'`, `llm_model TEXT NOT NULL DEFAULT 'qwen3:14b'`.
  Local-first default per operator mandate; anyone wanting cloud flips provider + sets the key.
- **Keys stay env-only** (`Settings`): existing `anthropic_api_key`; new `llm_api_key: str | None`
  (sent as `Authorization: Bearer` when set; Ollama ignores it). Keys must NOT enter
  `runtime_config` (its audit table stores old/new values in plaintext) and NOT
  `broker_credentials` (provider CHECK hard-pinned `'etoro'`, sql/018:47) — env is the settled
  cheap path; revisit only if multi-key management materialises.
- Fix config drift: `app/api/theses.py:47` stops reading `os.environ` directly; all resolution goes
  through `make_llm_client`.
- `.env.example`: document `ANTHROPIC_API_KEY` (currently live but undocumented) + `LLM_API_KEY`.

### 3. Audit columns — migration on `theses`

`ALTER TABLE theses ADD COLUMN model TEXT, ADD COLUMN provider TEXT, ADD COLUMN prompt_version TEXT;`
Populated at insert from the provider response + a new `_PROMPT_VERSION` constant in thesis.py
(bumped whenever `_WRITER_SYSTEM`/`_CRITIC_SYSTEM`/context shape changes). Nullable — historical
rows (none exist on dev) stay NULL. Additive, no `model_version` semantics touched.

### 4. Run/failure surface — `thesis_runs` table

```sql
CREATE TABLE thesis_runs (
  run_id BIGSERIAL PRIMARY KEY,
  instrument_id BIGINT NOT NULL REFERENCES instruments(instrument_id),
  trigger TEXT NOT NULL CHECK (trigger IN ('manual', 'cascade', 'scheduled')),
  started_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'running' CHECK (status IN ('running', 'ok', 'failed')),
  error TEXT,                         -- ValueError text + finish_reason on failure
  provider TEXT, model TEXT,
  thesis_id BIGINT REFERENCES theses(thesis_id)
);
```

One row per generation attempt, all three paths. Gives #1902 its in-flight indicator + failure
column and #1901 its cockpit feed with zero further backend work. Replaces log-only failure
handling (`scheduler.py:3156-3163`).

### 5. Force + auth on the manual path

- `POST /instruments/{symbol}/thesis?force=true` bypasses the 24h cache. Local-first makes the
  spend implication negligible; the force flag is recorded as `trigger='manual'` + noted in the run row.
- **Auth (fix in this work, first PR):** both thesis routers gain
  `dependencies=[Depends(require_session_or_service_token)]` (`app/api/auth.py:125`;
  precedent `app/api/bootstrap.py:60`). Today anyone with reach to :8000 can enumerate memos and
  burn LLM spend. Same-pass audit of other bare routers is out of scope (tracked in the issue).

### 6. Scheduled generation — revive as `thesis_refresh`

Re-register the dormant body in `SCHEDULED_JOBS` + `_INVOKERS` (fix the wiki row), renamed
`thesis_refresh`, **hourly**, with:

- **Scope:** held instruments ∪ top-N ranked (N=20 constant to start) ∪ the existing
  `find_stale_instruments` T1/T2 age/event predicate — intersected, then
- **Batch bound:** ≤5 generations per run, serial, semaphore-guarded, per-instrument advisory lock
  (existing `instrument_lock` pattern retained).
- Gate: provider resolvable (config present) — no longer `anthropic_api_key`-gated.
- The filing cascade (`daily_financial_facts` Phase 3) is unchanged apart from constructing its
  client via `make_llm_client`.

**Bootstrap/bulk first-load** (house rule: new data surface ships with bulk-tier first-load): the
hourly cadence × batch 5 IS the bounded bootstrap — 25-name first-load (held 5 + top-20) drains in
&lt;1 day, 674-name T1+T2 would drain in ~6 days without ever queuing a 17-40h monolith. No separate
bootstrap job.

### 7. Eval harness — `scripts/llm_eval_thesis.py`

Replayable fixtures: `_assemble_context` output captured from dev DB for the house panel
(AAPL, GME, MSFT, JPM, HD). The script runs writer+critic against the configured provider and
reports: JSON-schema pass rate (with/without the retry), enum validity, `finish_reason` mix,
tok/s + wall-clock per call. **Go-live gate:** ≥9/10 writer passes with retry on the chosen local
model, recorded in the impl PR. Re-run whenever `llm_model` changes — this is the token-free heavy
test loop the operator mandated.

### 8. Operator docs — external guide, linked not vendored

`docs/wiki/byo-llm.md` (thin, in-repo): where the config lives (`/config` + SettingsPage;
`LLM_API_KEY`/`ANTHROPIC_API_KEY` env), the smoke commands (`GET /v1/models`, one forced
generation), and a **link to the external operator guide** (model choice, Ollama install,
thinking-mode `/no_think` requirement, context-window sizing, quantisation tips). The guide itself
lives outside the repo per operator instruction; #1888's autonomy-engine `docs/byo-llm.md` is the
seed. The `finish_reason`/`/no_think` findings from this spec's empirical section move there.

## Implementation phasing (3 PRs)

1. **PR-A (core):** llm_client.py + providers + semaphore + lint; runtime_config migration +
   `/config`/SettingsPage knobs; theses audit-column migration; thesis_runs table; retry-once +
   think-strip + finish_reason; force param; router auth; `.env.example`; unify theses.py config
   resolution. Dev-verify: force-generate AAPL on local qwen3:14b, thesis renders on VerdictTab.
2. **PR-B (scheduling):** thesis_refresh registration (scope+batch), cascade switched to
   `make_llm_client`, wiki row fix. Dev-verify: watch two hourly runs drain the held+top-N queue;
   `thesis_runs` rows show ok/failed.
3. **PR-C (eval):** harness + fixtures + recorded benchmark of qwen3:14b vs deepseek-r1:14b.

## Out of scope (separate tickets)

- Context enrichment (current price anchor, `instrument_valuation`, IAR evidence block, TA block) —
  design-first; amends the settled prompt budget.
- Staleness v2 (price-drawdown / news-spike / fundamentals-delta regen predicates).
- Sentiment provider swap; #1902 Theses library FE; ta-analyst skill.

## Acceptance

- Zero Anthropic spend required for full operation: local endpoint end-to-end (writer+critic) on
  dev with theses visible on VerdictTab and picked up by the scoring confidence family after
  `compute_rankings`.
- Anthropic path still works when configured (provider flip + key) — verified once, cheaply.
- Every generation attempt has a `thesis_runs` row; every stored thesis carries
  model/provider/prompt_version.
- Settled thesis semantics untouched (versioning, enums, prompt budget, critic-always, #293 commit
  ordering, #273 triggers).
- `uv run pytest -m "not db"` + smoke green; new SQL mechanisms get one DB-tier test each
  (thesis_runs insert/read, runtime_config CHECK extension).
