# #3545 slice 1 — session-hours rate capture (the data a cost recalibration needs)

Ticket: #3545 (gap register P7). Rung: corpus change (new tables + a scheduled ingest job). Codex ckpt-1:
54 findings, disposition at the end.

## Premise check (dev DB, 2026-10-03)

The ticket gates recalibration on "the perishables recorder covering ≥ 20 sessions across all hours". As
scheduled, the recorder will not reach that:

- `etoro_perishables_snapshot` fires once a day, `Cadence.daily(hour=19, minute=7)` (`app/workers/scheduler.py`,
  `JOB_ETORO_PERISHABLES_SNAPSHOT`).
- Query: `select (observed_at at time zone 'UTC')::date, extract(hour from observed_at at time zone 'UTC'),
  count(*) from etoro_rate_observations group by 1, 2`. Every row is at UTC hour 19, except two first-day manual
  runs at 17 and 21. 9 snapshots, 25 Sep–2 Oct. Only a manual run or a boot catch-up lands anywhere else.
- So it repeats limit 1 of `cost_model.CALIBRATION_LIMITS`: one clock hour of the day (15:00 ET in summer,
  14:00 ET in winter).
- The other bid/ask histories cover 8 instruments (`strategy_quote_observations`) and 10
  (`strategy_core_quote_observations`). They are useful for execution checks on those names, but they cannot
  calibrate a price-band table over the universe. `quotes` holds one overwritten row per instrument.

This slice builds the capture that covers the session. The recalibration is slice 2, under a new
`COST_MODEL_ID`, once the wake condition below holds.

## Why the capture does not reuse the perishables tables

- `ranking_pot_activation.py` is hashed into `ranking_pot_policy` `POLICY_MODULES`. It reads "the latest
  `complete` `etoro_perishable_snapshots` row" and then that snapshot's eligibility rows. A rates-only snapshot
  would hand it no eligibility rows. **This forces separate storage.**
- `ai_trial_readout.py` is hashed into decl 16's policy. It marks a censored leg from "the latest
  `etoro_rate_observations` row observed by the censoring instant (15:00 UTC)". Its rule deliberately reads an
  evolving table, and rows stamped after an instant that has already passed cannot change a mark already taken.
  But intraday rows WOULD change which row every FUTURE censored mark uses, from a 19:07 row the day before to a
  14:37 row the same day. That would change a live trial's inputs mid-cohort, after its declaration froze, with
  no edit to the trial. Separate storage avoids that too.

The capture reuses `parse_rates`, `execute`, `_Phase` and `EtoroPerishablesProvider.get_rates` from the recorder
by import. These are not pure: `execute` does I/O and `_Phase` raises. Neither recorder's behaviour may move,
which the existing recorder tests pin.

## Population

- `load_validated_universe(conn)`, read once per capture. This is the cost model's own `CALIBRATION_POPULATION`:
  the model is applied to it, so it is what gets recorded.
- The 12,842-id sticky perishables universe would roughly double the requests for instruments the model never
  prices.
- **Membership is persisted.** Every request's `instrument_ids` is stored on the request ledger (below), so a
  missing id can be told apart as excluded, unattempted, errored or not served.
- The header stamps `VALIDATED_UNIVERSE_RULE_VERSION` and the size.
- The universe moves with `sync_universe`. That is slice 2's survivorship question (historical membership comes
  from the ledger, never from the then-current universe), and it is stated there.

## Schedule

- **When it fires:** `Cadence.hourly(minute=37)`. The ET–UTC offset is a whole number of hours in both DST
  regimes, so `:37` UTC is `:37` ET. The session fires are 09:37 … 15:37 ET: 7 on a full day, 4 on a half day
  (09:37–12:37).
- **What it misses:** the first 7 minutes after the open and the last 23 before the close. That is a named
  calibration limit for slice 2, not a hidden one. One minute per hour cannot cover both ends, and `:37` keeps
  the open-near hour, which is where daily-bar backtests fill.
- **Lane:** the `etoro_crowd` source lane, where the #3381 recorders run. No new lane is added, so no new pool
  connections (the job opens one `connect_job()` connection while it runs, as the recorders do).
- **Clear of the other fires on the shared quotas:**
  - `quotes_refresh` at :23 and `core_eligibility_refresh` at :20;
  - the daily recorder, 19:07 to ≤ 19:31 (measured 22m32s–23m00s over all 9 runs).
  - These are observed durations, not bounds. If the lane is busy, the capture queues behind it, and the start
    re-check below bounds how late it can run.
- **Prerequisite, checked at dispatch AND again at the start of the job body:** bootstrap complete, and the NYSE
  regular session open at that instant (`09:30 ≤ t < close` ET, with a 13:00 close on a half day, from
  `market_calendar.us_market_status`).
  - This is a new predicate. The halt window's `09:00 ≤ t ≤ close+15` is the wrong interval.
  - A fire queued past the close skips as a prerequisite skip and writes no header.
- No `catch_up_on_boot` and no `rearm_on_lost_fire`. A lost hour is absent, and the coverage query shows it.
- **Manual "Run now"** bypasses the dispatch prerequisite. The body re-check still applies, so an off-session
  manual capture skips rather than writing a row that looks in-session.

## Quota

- One capture is ⌈|universe| / 50⌉ rates GETs, about 135 today.
- The pacing is `_ETORO_RATES_INTERVAL_S = 1.1 s` on a `ResilientClient` that stamps every ATTEMPT, retries
  included. That is ≤ 55 attempts a minute from this instance.
- The process-wide market-data throttle (`_ETORO_READ_INTERVAL_S = 1.1 s` in `etoro.py`, shared by candles,
  `quotes_refresh` and the core 5-min refresh) is the other ≤ 55 a minute. Together they stay under the shared
  120/60 s.
- The two clocks are separate objects, which is why this bound is written out. The bound assumes no third
  concurrent instance. The daily recorder's rates phase is the only other per-instance F client, and the
  schedule above keeps the two apart (the lane serialises them).
- Per day: about 950 requests.

## Schema — `sql/465_etoro_session_rate_captures.sql`

**`etoro_session_rate_captures`** — one row per capture that reached its write, failed ones included:

- columns: `capture_id`, `started_at`, `finished_at` (= knowledge time; rows commit in one transaction at the end,
  so nothing is readable before it), `status` (`complete` / `partial` / `failed`), `recorder_version`,
  `universe_rule_version`, `universe_size`, `requests_expected`, `requests_ok`, `requests_errored`,
  `instruments_served`, `instruments_quoted`, `error`;
- `instruments_served` = observation rows; `instruments_quoted` = rows with `bid > 0 AND ask > 0 AND ask >= bid`,
  a census only, with no filter at write;
- counters: `requests_ok` and `requests_errored` are NOT NULL, ≥ 0, and `ok + errored ≤ expected`;
  `requests_expected` is NULL only on a `failed` capture that never planned;
- `failed ⇔ error IS NOT NULL`. A `partial` run's diagnostics live on its errored request rows;
- `complete ⇒ requests_ok = requests_expected`;
- `partial ⇒ ok + errored = expected AND errored > 0`.

**`etoro_session_rate_requests`** — the ledger:

- columns: `(capture_id, seq)` PK, `instrument_ids BIGINT[]`, `observed_at`, `outcome` (`ok` / `error`),
  `http_status`, `error_body JSONB`;
- `error_body` is the body as served, or the transport error's text, on an `error` row (envelope violations
  included), and NULL on `ok`.

**`etoro_session_rate_observations`**:

- columns: `(capture_id, instrument_id)` PK, `request_seq`, `observed_at`, `quote_at`, `bid`, `ask`,
  `last_execution`, `conversion_rate_bid`, `conversion_rate_ask`;
- FK `(capture_id, request_seq)` → the ledger;
- index on `(instrument_id, observed_at)`;
- the same projection as `etoro_rate_observations`: published units as served, NULL when absent, no validity
  filter.

All three refuse UPDATE through `etoro_crowd_append_only()`.

**Raw `ok` bodies are not stored.** This departs from the recorder, deliberately:

- Volume: about 6.7k × 7 × 252 ≈ 11.8M observation rows a year, at ~180 B per row. That figure is
  `pg_total_relation_size('etoro_rate_observations')`, indexes included: 21 MB over 115,463 rows. So ≈ 2 GB a
  year.
- The projection keeps every field the recorder keeps.
- An entry with no usable `instrumentID` is dropped, exactly as the recorder drops it.
- Envelope violations and HTTP errors keep their body on the ledger.
- What is lost is drift inside an otherwise-valid ok body. The daily recorder's raw bodies (same endpoint,
  same client) remain the drift sample. That is a stated limit, not equivalence.

No retention: the data cannot be re-fetched.

## Failure semantics (as the recorder)

- **Universe:** an empty universe, or one over `MAX_UNIVERSE`, is refused and writes a `failed` header.
- **Systemic refusals:** 401/403, `MAX_CONSECUTIVE_ERRORS`, or no request ok. These write a `failed` header with
  the ledger and rows collected so far, then raise.
- **Partial:** a `partial` capture commits, then raises `LayerRefreshFailed` with `partial_category` over its
  errored requests (category precedence as in the recorder).
- **Unexpected exception:** writes a `failed` header and re-raises.
- **Limit:** process death before the write leaves nothing, as in the recorder.

## Acceptance

- **Pure tests** (session predicate), on actual UTC instants in EDT and EST:
  - 09:29:59 closed and 09:30 open;
  - 15:59:59 open and 16:00 closed;
  - half day 12:59:59 open and 13:00 closed;
  - weekend and holiday closed.
- **DB tests** (fake source):
  - a complete capture writes header, ledger and rows;
  - one errored batch (HTTP 500 and an envelope violation) → `partial`, the ok rows kept, the error bodies kept,
    and the run raises;
  - 401 → `failed`;
  - an empty universe → `failed`;
  - an off-session start → skip, with no header.
- **Dev, after merge + daemon restart:** one in-session capture (the next session's first `:37` fire, or a manual
  run inside the session). Report the header (`universe_size`, `instruments_served`, `instruments_quoted`).
  Spot-check one instrument against the daily recorder's nearest 19:07 row by comparing `quote_at` and bid/ask
  units (`quotes` is a weaker oracle: its `quoted_at` may be older than the read).

## Wake condition for slice 2 (the recalibration)

A full NYSE session counts when it has a `complete` capture whose `started_at` falls in every one of its
scheduled ET clock hours: 09–15 on a full day, 09–12 on a half day. Slice 2 starts when **≥ 20 sessions are
full**. Run it with `scripts/report_3545_session_rate_coverage.py`, shipped in this slice: it prints the count
of full sessions, and per-session coverage, using `market_calendar` for the denominator. The scheduler produces
the coverage with no attended step. A loop pass runs the script to check it.

## Slice 2 must decide (recorded so they are not rediscovered)

- **The spread metric:** `(ask − bid) / mid` from the same row, with the band chosen on that mid (as-traded).
  `quote_at` freshness is required to be inside the session and within N minutes of `observed_at`. N must come
  from a source rule, or be fixed by construction and frozen in the id.
- **Weighting:** one observation per instrument per session-hour. Extra manual captures in the same hour are
  dropped before the percentile.
- **Failed and partial captures:** count them, and check whether their surviving rows are wider. Missing
  batches may be stress, not noise.
- **Named limits:** the unobserved first 7 / last 23 minutes; no volatile regime unless the window holds one;
  hourly rows are correlated, so n is not 7 × instruments × sessions in information terms; the `<$5` band's
  distinct-instrument n.
- **Membership:** from the ledger at capture time, never from the then-current universe.

## Not in this slice

- **The recalibration** (slice 2, above).
- **Effective spread vs mid per engine fill.** Measured: 8 engine opens in the last 45 days
  (`broker_positions` ∪ `broker_positions_closed`), and none has a bid/ask observation within 6 minutes before
  it. A per-fill mid needs a quote captured at submission, inside the executors. Those are hashed policy modules
  for live declarations, so this must be a new executor version, never an edit. The hourly capture is no
  substitute, and this slice does not claim one.

## Codex ckpt-1 disposition (54)

**Applied:**

| findings | what changed |
| --- | --- |
| 1–4, 6–8 | premise wording; explicit UTC in the query; season-dependent hour; the ranking reader as the forcing reason; future vs past trial marks; "pure" dropped |
| 9, 31, 34, 35 | request ledger with membership and error bodies |
| 13–17, 52 | session predicate, half day = 4 fires, the body re-check, boundary tests |
| 18 | minute moved to `:37` and the blind spot named |
| 20, 25 | durations stated as observations, with lane queueing and the start re-check |
| 23 | per-attempt pacing verified |
| 26–30 | CHECK contradiction removed; counters NOT NULL; empty/over-cap universe refused; `instruments_quoted` census |
| 38 | unexpected-exception path |
| 39 | knowledge time |
| 41, 42, 44–46 | executable full-session wake via a shipped script |
| 47–51 | slice-2 requirements; the ≤ 60 min mid claim removed |
| 53 | failure tests |
| 54 | oracle |

**Accepted as stated limits:** 5, 10–12, 21, 22, 24, 32, 33, 36, 37, 43.

**Not acted on:** 19 (batch-order confounding). A capture spans ~2.5 min inside an hour bucket, and the ledger
records each batch's own `observed_at`.

**Deferred to slice 2:** 40 (manual/scheduled provenance). Slice 2's one-per-instrument-per-hour rule makes a
repeat manual capture harmless.
