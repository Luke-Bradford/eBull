# BYO LLM — thesis-engine provider configuration (#1919)

eBull's thesis engine plugs into any OpenAI-compatible completion
endpoint (the #1888 contract). Default is **local-first**: Ollama at
`http://localhost:11434/v1` with `qwen3:14b` — no API key, no cloud
spend. Anthropic remains available by configuration.

## Where the config lives

- **Knobs** (`llm_provider` / `llm_base_url` / `llm_model`): DB-backed
  `runtime_config` singleton — edit on the **Settings page → LLM
  Provider**, or `PATCH /config` (audited per field in
  `runtime_config_audit`).
- **Keys** (env-only, never in the DB — the audit table is plaintext):
  - `ANTHROPIC_API_KEY` — required only when `llm_provider='anthropic'`.
  - `LLM_API_KEY` — optional bearer for OpenAI-compatible endpoints that
    need one (Ollama ignores it).

## Smoke commands

```bash
# Endpoint alive + models present:
curl -s http://localhost:11434/v1/models | jq '.data[].id'

# One forced generation (auth: service token or browser session):
curl -s -X POST -H "Authorization: Bearer $EBULL_SERVICE_TOKEN" \
  "http://localhost:8000/instruments/AAPL/thesis?force=true" | jq '.cached, .thesis.stance'

# Attempt log (in-flight / ok / failed + finish_reason in error):
# SELECT * FROM thesis_runs ORDER BY run_id DESC LIMIT 5;
```

## Scheduled generation (#1919 PR-B)

The `thesis_refresh` job runs **hourly at :07** (jobs daemon): held
positions ∪ top-20 ranked, filtered by the staleness predicate (no
thesis / `review_frequency` elapsed / superseding 10-K/10-Q/8-K),
**≤5 generations per run**, serial. The hourly cadence × batch bound is
the bounded bootstrap drain — a 25-name first load drains in under a
day; there is no separate bulk job. Skips (PREREQ_SKIP) only when
`llm_provider='anthropic'` with no `ANTHROPIC_API_KEY`; the local-first
default always runs. Manual fire: Admin → Processes → Run now, or
`POST /jobs/thesis_refresh/run`. Every attempt lands in `thesis_runs`
(`trigger='scheduled'`).

## Model gotchas (empirical, 2026-07-09)

- **qwen3 thinking mode** burns the whole completion budget
  (`finish_reason=length`, empty content). The provider layer appends
  `/no_think` to the system prompt and strips any `<think>…</think>`
  block defensively — but pick models with the eval harness before
  switching `llm_model`:

  ```bash
  # Replay the house-panel fixtures (AAPL/GME/MSFT/JPM/HD) against a
  # candidate model; go-live gate = >=9/10 writer passes with retry.
  PYTHONPATH=. uv run python scripts/llm_eval_thesis.py run \
    --models qwen3:14b <candidate-model> --gate-model <candidate-model>
  # Re-capture fixtures from the dev DB (dev-guarded):
  PYTHONPATH=. uv run python scripts/llm_eval_thesis.py capture
  ```

- A failed generation records `finish_reason` in `thesis_runs.error` —
  `length` means truncation (context/output window too small), `stop`
  with a JSON error means the model can't hold the schema.
- **deepseek-r1 needs the fence normalization** (benchmark 2026-07-09,
  #1919 PR-C): Ollama does not enforce `response_format=json_object`
  for it, and it often wraps otherwise schema-valid JSON in a
  ` ```json ` fence. Since PR-C the provider strips one whole-text code
  fence (model-neutral, lossless — same class as the `<think>` strip),
  which flipped deepseek-r1:14b's writer gate from 4/10 FAIL to 10/10
  PASS. It also intermittently answers with a free-prose markdown memo
  (no JSON at all) — deliberately NOT recovered; the retry absorbs it.
  qwen3:14b stays the default: critic reliability 10/10 vs 9/10 and
  enum validity 100% vs 87%, though deepseek is ~2.5× faster on this
  hardware (7.9 vs 3.1 tok/s).

The full operator guide (model choice, Ollama install, context-window
sizing, quantisation tips) lives outside this repo — seeded from the
autonomy-engine `docs/byo-llm.md` (#1888).
