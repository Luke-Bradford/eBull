---
name: thesis-writer
description: eBull thesis engine — the Claude-written versioned investment memo (writer + critic) in app/services/thesis.py, the theses table, its endpoints/jobs, and how its fields feed scoring and the portfolio manager.
---

# thesis-writer

## When to use

Any change to `app/services/thesis.py`, the `theses` table (`sql/001_init.sql`,
`sql/006_theses_critic_json.sql`), the thesis API (`app/api/theses.py`), the
`daily_thesis_refresh` job or `cascade_refresh` (`app/services/refresh_cascade.py`),
or the prompt/context caps. Also read it before touching how scoring (`scoring.py`) or
the portfolio manager (`portfolio.py`) consume thesis fields (`confidence_score`,
`base_value`/`bear_value` bands, `stance`, `break_conditions_json`). Critic-lens changes
belong to the neighbouring `thesis-critic` skill.

## What it is

Build priority #4 (`.claude/CLAUDE.md`). `generate_thesis(instrument_id, conn, client)
-> ThesisResult` (`thesis.py`) assembles a capped research context, calls the Claude
**writer** for a structured memo, calls the Claude **critic** for a counter-thesis, and
appends one new versioned row to `theses`. Model `claude-sonnet-4-6` (`_MODEL`); client
from `make_anthropic_client` (`app/services/anthropic_client.py`).

- `_assemble_context` pulls capped inputs: latest 1 prior thesis, 3 filing_events (with
  `extracted_summary`), 5 fundamentals_snapshot (latest + 4), 10 news_events from the last
  30d, instrument metadata, and the `risk_v1` evidence block from
  `instrument_risk_metrics_current` via `_shape_risk_metrics`. `earnings_events` +
  `analyst_estimates` retired with FMP (#539) — the writer tolerates their absence.
- `_call_writer` → validated JSON (`_validate_writer_output`): `thesis_type`,
  `confidence_score` [0,1], `stance`, `buy_zone_low/high`, `base/bull/bear_value`,
  `break_conditions`, `memo_markdown`. Raises on schema-invalid output (blocks insert).
- `_call_critic` → `critic_json` (`_validate_critic_output`): summary, key_risks,
  hidden_assumptions, evidence_gaps, thesis_breakers, verdict. **Best-effort** — any
  failure logs and stores the thesis without `critic_json`; never blocks the insert.
- `_insert_thesis_atomic` computes `thesis_version` inside the INSERT via
  `COALESCE(MAX(thesis_version),0)+1`; `UNIQUE(instrument_id, thesis_version)` is the
  final guard. Then `_update_last_reviewed` sets `coverage.last_reviewed_at`.
- `find_stale_instruments(conn, tier=1, *, instrument_ids=None)` flags instruments with
  no thesis, unknown `review_frequency`, a newer qualifying filing (`event_new_10k/10q/8k`,
  #273), or a cadence-expired thesis. Every returned instrument passes
  `coverage.filings_status = 'analysable'` (#268).

**Table `theses`** (`sql/001_init.sql:87`): `thesis_id` PK, `instrument_id` FK,
`thesis_version`, `created_at`, `thesis_type`, `confidence_score NUMERIC(10,4)`,
`stance`, `buy_zone_low/high`, `base/bull/bear_value NUMERIC(18,6)`,
`break_conditions_json JSONB`, `memo_markdown TEXT NOT NULL`, `critic_json JSONB`
(`sql/006`); `UNIQUE(instrument_id, thesis_version)`; index `idx_theses_instrument_created`.

**Endpoints** (`app/api/theses.py`, mounted `app/main.py:562-563`): `GET /theses/{instrument_id}`
(latest; **200 + null** when no thesis, #1813), `GET /theses/{instrument_id}/history`
(paginated, newest first), `POST /instruments/{symbol}/thesis` (generate-or-cached,
`THESIS_CACHE_WINDOW = 24h`; 503 if `ANTHROPIC_API_KEY` unset, 404 unknown symbol,
502 on generation failure).

**Jobs**: `daily_thesis_refresh` (`JOB_DAILY_THESIS_REFRESH`,
`app/workers/scheduler.py`) refreshes stale T1/T2 via `find_stale_instruments`; skips if
no API key. `cascade_refresh` re-generates theses for instruments touched by fresh
filings, then re-runs `compute_rankings` if any refreshed — session advisory-locked
against `daily_thesis_refresh` so the two never double-run one instrument.

**How theses feed scoring** (`scoring.py`, read at 1168-1179: latest row's
`confidence_score, base_value, bear_value, created_at`): the **value family**
(`_value_score`) uses `base_value`/`bear_value` (upside-to-base vs downside-to-bear),
falling back to fundamentals + price-target when `base_value` is null; the **confidence
family** reads `confidence_score`; additive **penalties** apply `stale_thesis` −0.15 when
older than `_THESIS_STALE_DAYS` (90d) and `low_confidence` −0.10 when
`confidence_score < _LOW_CONFIDENCE_THRESHOLD` (0.40) — a *missing* thesis is NOT penalised
(would block T3→T2 promotion). Thesis also carries 0.15 of completeness `C`, full credit ≤
`_C_THESIS_FRESH_DAYS` (90d) (evidence-only; see `ranking-engine`).

**Portfolio consumption** (`portfolio.py`): BUY needs `stance == "buy"` OR
`total_score >= MIN_SCORE_ONLY_BUY` (0.55); ADD needs conviction improvement
(`ADD_MIN_SCORE_DELTA` 0.05, settled ADD rule); EXIT fires on `break_conditions_json` /
severe red flag / `current_price >= base_value`.

## Invariants

- **Thesis versioning** (settled-decisions "Thesis semantics"): each generation inserts a
  new row; never overwrite prior rows. `theses` is append-only.
- **Critic output** stored in `critic_json` only — do NOT append critic text into
  `memo_markdown`. Critic runs on every generation in v1.
- **Allowed thesis types** (`compounder`/`value`/`turnaround`/`speculative`) and **stances**
  (`buy`/`hold`/`watch`/`avoid`) are enforced by `_VALID_*` frozensets + validators — no
  ad-hoc strings.
- **Thesis freshness** is the latest row's `created_at` + `coverage.review_frequency`
  (`daily`=1/`weekly`=7/`monthly`=30); `coverage.last_reviewed_at` is operational metadata,
  NOT primary truth. **Prompt budget** capped in v1 (1 prior / 3 filings / 5 fundamentals /
  10 news @ 30d) — a settled decision, not a tuning knob.
- **Caller contract** (`generate_thesis`): do NOT wrap in `with conn.transaction()`. It
  commits the read tx before the Claude calls (2–10s each) so the connection is not
  `idle in transaction` across HTTP; psycopg3 forbids `commit()` inside an outer block.
- **Auditability / deterministic execution** (repo non-negotiables): the thesis is the
  AI-heavy research layer; it *feeds* scoring/portfolio but never bypasses the hard-rule
  execution guard. The versioned memo + bands + critic are the persisted audit trail.

## Failure conditions

Missing critical source data, stale timestamps beyond threshold, or contradictory
evidence must surface as explicit signals — never a neutral default.
- **Missing data stays missing**: `_shape_risk_metrics` returns `None` when metrics were
  never computed; `_to_float` keeps NULLs as `None`, never a fabricated 0; retired
  enrichment passes as null. In `_value_score` a present `base_value` with a missing
  `bear_value` records a note + explicit downside penalty, not a silent fill.
- **Staleness is penalised, not hidden**: an outdated thesis draws the additive
  `stale_thesis` deduction and `find_stale_instruments` re-queues it (cadence + event-driven).
- **Contradiction is surfaced**: the critic emits an explicit `verdict`; non-`ok` risk
  statuses pass through verbatim and the prompts forbid citing them as precise numbers.
- **Writer failure blocks the insert** (raises); **critic failure is tolerated** (row
  stored without `critic_json`, logged) — never let a best-effort critic wedge a valid memo.
