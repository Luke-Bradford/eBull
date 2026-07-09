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

## Model gotchas (empirical, 2026-07-09)

- **qwen3 thinking mode** burns the whole completion budget
  (`finish_reason=length`, empty content). The provider layer appends
  `/no_think` to the system prompt and strips any `<think>…</think>`
  block defensively — but pick models with the eval harness (#1919
  PR-C) before switching `llm_model`.
- A failed generation records `finish_reason` in `thesis_runs.error` —
  `length` means truncation (context/output window too small), `stop`
  with a JSON error means the model can't hold the schema.

The full operator guide (model choice, Ollama install, context-window
sizing, quantisation tips) lives outside this repo — seeded from the
autonomy-engine `docs/byo-llm.md` (#1888).
