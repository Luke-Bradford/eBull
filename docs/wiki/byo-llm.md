# BYO LLM — thesis-engine provider configuration (#1919)

eBull's thesis engine plugs into any OpenAI-compatible completion
endpoint (the #1888 contract). Default is **local-first**: Ollama at
`http://localhost:11434/v1` with `qwen3:14b` — no API key, no cloud
spend. Anthropic remains available by configuration.

## Where the config lives

- **Knobs** (`llm_provider` / `llm_base_url` / `llm_model_writer` /
  `llm_model_critic`): DB-backed `runtime_config` singleton — edit on
  the **Settings page → LLM Provider**, or `PATCH /config` (audited per
  field in `runtime_config_audit`). Writer and critic are SEPARATE
  model knobs (#1995) — the bulk memo writer and the adversarial critic
  may run different local models; provider and base URL stay shared.
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
  switching either model knob:

  ```bash
  # Replay the house-panel fixtures (AAPL/GME/MSFT/JPM/HD) against a
  # candidate model; go-live gate = >=9/10 writer passes with retry.
  # --critic-model mirrors the production split (#1995): critic rounds
  # run on the configured critic while writers are swept.
  PYTHONPATH=. uv run python scripts/llm_eval_thesis.py run \
    --models qwen3:14b <candidate-model> --critic-model qwen3:14b \
    --gate-model <candidate-model> --json-out /tmp/llm_eval_results.json
  # Content-grading judge (#1995): the structural gate above checks only
  # schema validity; the judge compares two writers' memos on identical
  # fixtures (blinded A/B, order-swapped ×2, ctx-overflow guarded):
  PYTHONPATH=. uv run python scripts/llm_eval_thesis.py judge \
    --results /tmp/llm_eval_results.json \
    --model-a qwen3:14b --model-b <candidate-model> --judge-model qwen3:14b
  # Re-capture fixtures from the dev DB (dev-guarded):
  PYTHONPATH=. uv run python scripts/llm_eval_thesis.py capture
  ```

- A failed generation records `finish_reason` in `thesis_runs.error` —
  `length` means truncation (context/output window too small), `stop`
  with a JSON error means the model can't hold the schema.
- **⚠ Ollama serves a 4,096-token context by default** — far below the
  #1987 enriched writer input (~3-3.5k prompt tokens + ~1.2k system +
  2,048 output budget), and with `--context-shift` it silently DROPS the
  oldest prompt tokens instead of erroring: the writer loses the context
  head with `finish_reason: stop` and no visible failure. Set
  `OLLAMA_CONTEXT_LENGTH=16384` on the serve process (brew launchd:
  `EnvironmentVariables` in `~/Library/LaunchAgents/homebrew.mxcl.ollama.plist`,
  then `launchctl unload`/`load` — NOT `brew services restart`, which
  regenerates the plist and wipes custom env). Verify live via
  `curl -s localhost:11434/api/ps` → `context_length` after any request
  (found 2026-07-10: llama-server ran `-c 4096` while fixtures already
  exceeded it pre-#1987).
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
