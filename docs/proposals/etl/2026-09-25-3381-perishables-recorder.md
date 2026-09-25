# #3381 slice 3 — perishable trading-conditions recorder (rates, eligibility, what-if costs)

Status: proposal (2026-09-25), revised after Codex ckpt-1 (85 findings; numbers cited as `#n`). Parent: #3381
(slice 1 crowd snapshot `3842a878`, slice 2 investor cohort `f700dc82`). Scope from the #3381 comment of
2026-09-25 10:49Z: "record, forward-only on the same cadence: quotes/spreads (the rates endpoint), eligibility
incl. short availability and what-if costs for the tracked universe".

## Why

eToro serves none of these as history. `quotes` is one mutable row per instrument (`market_data.refresh_quotes`
docstring) over held + T1/T2 + benchmarks + core only; `strategy_quote_observations` samples ≤ 50 research
instruments; eligibility and what-if are called per action (`strategy_paper_executor`, `strategy_core_executor`,
`strategy_core_broker_preflight`, `strategy_position_manager`) and never recorded as a cross-section. So "was
X shortable at x1 on day D, at what spread and quoted cost" is unanswerable after D.

## Source rule

Committed OpenAPI `tests/fixtures/etoro/openapi_v1.375.0.json` (`x-ratelimit`, schemas) and
`.claude/skills/data-sources/etoro-api.md` §preflight (lines 33-185).

| endpoint | quota | our lane | per request |
| --- | --- | --- | --- |
| `GET /api/v1/market-data/instruments/rates?instrumentIds=` (`LiveRatesResponse`) | 120/60 s shared, 11 members | F_market_data | ≤ 100 ids; we send 50 (`_RATES_BATCH_SIZE`) |
| `POST /api/v2/trading/info/demo/eligibility` (`InstrumentEligibilityResponse`) | 20/60 s dedicated per endpoint | B_eligibility | ≤ 100 ids |
| `POST /api/v2/trading/info/demo/costs` (`GetCostResponse`) | 20/60 s dedicated per endpoint | C_what_if_costs | 1 order |

- **Rates**: `instrumentID`, `bid`, `ask`, `lastExecution`, `conversionRateBid/Ask`, `date` ("the date-time of
  the price in the system").
- **Eligibility** `LeverageConfiguration`: `settlementType ∈ {cfd, real, realFutures, marginTrade}`, `direction
  ∈ {long, short}`, `leverageValues`, `isPotential` ("additional user questionnaire may be required to allow
  the user to trade with this setup"). Eligibility is account-specific (portal).
- **What-if**: per the skill, a cost row's `value` is denominated in THAT ROW's `currency` (#1); the
  vocabulary is open; an absent row is not zero and an absent key is not a null (#7); `markup`/`overnightFee`
  zero-only observations and x1 short CFDs have no decoded unit (#2); a $1,000 ticket's 0.01 quantum is 1 bp,
  so observations under ~10 bp of cost are rounding-dominated (#4); `marketSpread` vs a quoted spread is an
  order-of-magnitude agreement against the FULL spread, not an identity, and both come from one venue so
  they do not validate each other (#5/#6); an OPEN-arm cost never bounds the CLOSE arm, and the close arm
  needs a real `positionIds`, so this recorder cannot cost exits (#3). Probe 2026-09-25 15:28Z (demo, three
  calls, not a population claim): `sellShort/cfd` on GME and AAPL returned a `transactionFee` row (1.5 USD)
  that the `buy/real` AAPL call did not.

**Relation to the skill's "no recurring cost writer is justified by this evidence"** (`etoro-api.md` lines
48-53, 2026-08-09). That sentence answered whether what-if output could be an EXECUTION cost input — zero of
20 responses were execution-usable, so no writer should feed the cost model. This recorder does not feed one:
it stores the responses as dated research evidence, which is what the #3381 scope comment asks for, and the
skill's "keep live strategy activation refused while what-if costs return undocumented fields" is untouched.
The skill text is amended on #2403 (`.claude/**` is write-refused from the loop worktree).

**Local projections (NOT source rules, #8)** — named constants in `RECORDER_VERSION`, raw kept for re-derivation:
- `short_x1` / `long_x1` ∈ {`available`, `potential`, `absent`, NULL}: over configs with the given direction and
  `1 ∈ leverageValues` — any with `isPotential = false` → `available`; else any with `isPotential = true` →
  `potential` (#29); none → `absent`; any such config with a missing/malformed `isPotential`, direction,
  settlement or leverage array → NULL (#28). Short considers `cfd` configs only: on eToro a short is a CFD
  (`.claude/CLAUDE.md` risk posture). These are configuration capacity; `allow_open_position` is its own column
  and is NOT folded in (#31).
- `long_x1_settlement`: settlement of the first `available` long x1 config in the total order `real, cfd,
  realFutures, marginTrade` (#30).
- `max_short_leverage`: max `leverageValues` over non-potential `cfd` short configs; NULL if none, or if an
  unreadable config could be a non-potential `cfd` short. An unreadable config makes unknown only the
  projections it could belong to: a readable direction, settlement or leverage array that rules it out of an arm
  leaves that arm alone (Codex ckpt-3).
- Numbers: JSON int/float/numeric-string → NUMERIC; bool, non-finite, anything else → NULL (#40). Timestamps:
  timezone-aware ISO-8601 → value; else NULL (#60). No validity filter on bid/ask (zero, crossed kept, #41);
  spread is a READER computation — the skill's convention is the full spread `(ask − bid)` over mid (#42).

## Universe (fixed by construction; no published rule)

- **Rates + eligibility — sticky** (#9/#10/#11): `etoro_perishable_universe` ledger `(instrument_id PK,
  first_snapshot_id)`. Each run inserts every current `instruments.is_tradable` id (`ON CONFLICT DO NOTHING`),
  then records the WHOLE ledger — so an instrument that stops being tradable, or is delisted, keeps being asked
  about (eligibility answers `notFound`; rates omit it — both recorded). Universe of snapshot S = ledger rows
  with `first_snapshot_id ≤ S`, reconstructible after an abort. Ordered by `instrument_id`. Empty universe →
  `failed` (#12). Above `MAX_UNIVERSE = 30,000` → `failed` before any request, ledger insert still committed
  (#13); the refusal is loud and the fix is a new version. 12,827 tradable on 2026-09-25 (`select count(*)
  filter (where is_tradable) from instruments`).
- **What-if panel — sticky, capped** (1 request per order at 20/min cannot cover 12,827 × 2 arms daily ≈ 21 h).
  `WHATIF_PANEL_RULE = "cohort-long50-short50-sticky-v1"`. Source = latest `etoro_investor_snapshots` row with
  `status ∈ {complete, partial}` and `finished_at ≥ run start − 48 h`, ordered `(finished_at desc,
  snapshot_id desc)`, read once at run start (#22/#23/#24). Selection = top 50 instruments by distinct `cid`s
  holding a top-level LONG position ∪ top 50 by distinct `cid`s holding a top-level SHORT position (#17), ties
  by `instrument_id`. Additions go to ledger `etoro_whatif_panel (instrument_id PK, first_snapshot_id,
  source_investor_snapshot_id, long_holders, short_holders)`; the costed panel is the whole ledger (#19), capped at
  `MAX_WHATIF_PANEL = 250` instruments (≤ 500 requests ≈ 29 min): once full, new additions are refused and
  counted in `whatif_panel_refused` (never evict). No qualifying source → no additions, `whatif_panel_source_id`
  NULL, existing panel still costed (#25).
  ⚠ Known properties, stated not fixed (#14/#15/#16/#18/#20/#21): a costing panel, not a sample — it inherits
  the PI/public-profile/copier selection of slice 2, weights holders equally, counts top-level positions only
  (copied `socialTrades` are excluded), ignores size/leverage, breaks ties toward lower ids, and an unavailable
  member contributes no holdings.
- **Arms** (panel order = ledger `first_snapshot_id, instrument_id`; long before short, #62): `$1,000` USD,
  `action=open`, `leverage=1`, `orderType=mkt`. `long` = `buy` at `long_x1_settlement`; `short` = `sellShort` at
  `cfd` iff `short_x1 = available`. Per arm, decided from THIS run's eligibility: instrument answered `notFound`
  or `allow_open_position` false → `not_eligible`; config not `available` → `not_offered`; no eligibility answer
  (an errored request or an omitted id), or an unreadable `allow_open_position` / arm config → `undecided`;
  otherwise planned. A planned arm is `ok` / `error`, or
  `unattempted` if the run aborted first (#47/#48). A request eToro refuses (e.g. under `minPositionExposure`)
  is an `error` with its status and body — data, not a crash (#32).

## Schema (`sql/426_etoro_perishables.sql`) — UPDATE refused by trigger on every table (sql/424 pattern)

- `etoro_perishable_snapshots` — `snapshot_id` bigserial, `started_at`, `finished_at` (#43), `status ∈ {complete,
  partial, failed}`, `recorder_version`, `request_params` (incl. `environment`, #56), `universe_size`,
  `whatif_panel_source_id`, `whatif_panel_added`, `whatif_panel_refused`, per phase `{eligibility, whatif,
  rates}_{expected, ok, errored}` (NULL until the phase is planned), `error`. CHECKs (#46): counters ≥ 0;
  `ok + errored ≤ expected`; `complete` ⇔ not failed ∧ every phase `ok = expected`; `partial` ⇔ not failed ∧
  every phase `ok + errored = expected` ∧ some `errored > 0`; `failed` ⇔ `error` set.
- `etoro_perishable_requests` — `request_id` bigserial PK (#44), `snapshot_id`, `phase`, `seq`, UNIQUE
  `(snapshot_id, phase, seq)` and `(request_id, snapshot_id, phase)`; `instrument_ids bigint[]`, `request_body
  jsonb`, `observed_at` (response receipt after the client's retries, or when the error surfaced),
  `outcome ∈ {ok, error}`, `http_status` (any response received, including a malformed 200; NULL only for a
  transport error, #53), `raw jsonb` (the parsed body in full; for an unparseable or non-JSON body, the full
  text as a JSON string; for a transport error, the exception text, #54). JSON is semantic, not byte-exact
  (#57). One row per LOGICAL request; per-attempt retries are already counted by the #2946 attempt observers,
  not here (#52).
- `etoro_rate_observations` — PK `(snapshot_id, instrument_id)`; `(request_id, snapshot_id, 'rates')` FK (#45);
  `observed_at`, `quote_at`, `bid`, `ask`, `last_execution`, `conversion_rate_bid`, `conversion_rate_ask`.
- `etoro_eligibility_observations` — PK `(snapshot_id, instrument_id)`; FK as above for `'eligibility'`;
  `observed_at`, `answer ∈ {found, not_found}`, `allow_open_position`, `allow_close_position`,
  `min_position_exposure`, `max_units_per_order` (NULL when missing/malformed, never false), `long_x1`,
  `short_x1`, `long_x1_settlement`, `max_short_leverage`.
- `etoro_whatif_observations` — PK `(snapshot_id, instrument_id, arm)`; `outcome ∈ {ok, error, unattempted,
  not_offered, not_eligible, undecided}`; `eligibility_request_id` (the answer the decision read, NULL for
  `undecided` from an omission, #50); `request_id` NOT NULL ⇔ outcome ∈ {ok, error}, FK for `'whatif'`;
  `transaction`, `settlement_type`, `amount_usd`, `leverage` (NULL unless planned); `last_updated`. Cost rows
  are read from the request's `raw`, deliberately not re-projected (#38).

**Envelope rules — a violation makes that request `error`, raw kept, no parsed rows** (#26/#27/#35/#37/#39):
rates body not an object or `rates` not a list; a rates entry with a duplicate or unrequested `instrumentID`.
Eligibility missing `eligibilities`/`notFoundInstrumentIds` arrays; any id duplicated, unrequested, or both found
and not found. What-if `instrumentId` ≠ requested, or `costs` not a list. An entry with no usable
`instrumentID` is skipped and counted in the request's raw only (#55). A requested id neither found nor
not-found is OMITTED: no eligibility row, derivable by anti-join against `instrument_ids` (#26).

**Clocks** (#58/#59/#64/#65/#66): `observed_at` = response receipt; knowledge time of the whole snapshot =
`finished_at` (commit happens at the end). `quote_at` is the provider's price clock and may be stale; the
what-if `last_updated` likewise (per-instrument freshness, skill). A no-request what-if row's clock is the
referenced eligibility request's `observed_at`. Readers join signals by `observed_at ≤ signal time`, never by
calendar date (this job runs before the 21:52/22:07 recorders). One observation per day: it says nothing about
intraday restrictions or spikes.

## Failure semantics (slice 2's pattern, #75-#79)

- Per request: non-2xx after retries, transport error, or envelope violation → `error`; the run continues.
- **Systemic → the WHOLE run stops (all later phases) and is `failed`**: universe empty or over cap; a 401 or
  403 on any request (credential-wide, #76); `MAX_CONSECUTIVE_ERRORS = 10` errored requests in a row within a
  phase; a phase that ends with requests > 0 and ok = 0; any other exception (#77). A failed run commits its
  header TOGETHER with the universe/panel ledger inserts, every request and parsed row collected, and
  `unattempted` rows for planned arms; if that write fails, a bare `failed` header is attempted best-effort
  (#78). The original exception is re-raised.
- `partial` is committed, then `PerishableSnapshotPartial` is raised OUTSIDE the failure handler (#79), so
  `job_runs` never reads `success` for lost data. A hard crash leaves no rows; `job_runs` shows the failure
  and the next fire records a new snapshot, it cannot recover the lost one (#80).
- `complete` measures collection, not coverage (#51): coverage is the counters plus the anti-joins above.
- Both raised types carry a `FailureCategory` (Codex ckpt-2 P1), so `classify_exception` does not flatten a
  credential refusal or a rate limit to `INTERNAL_ERROR`: 401/403 → `AUTH_EXPIRED`; 429 → `RATE_LIMITED`;
  5xx/transport → `SOURCE_DOWN`; a 2xx envelope violation → `SCHEMA_DRIFT`; other 4xx → `INTERNAL_ERROR`. Over
  several errored requests the most operator-actionable category wins, in that order.

## Job

`etoro_perishables_snapshot`, daily **19:07 UTC**, `prerequisite=_bootstrap_complete` (#81). 19:07 UTC is
inside NYSE regular hours on a full trading day in both DST regimes (15:07 EDT / 14:07 EST); weekends,
holidays and early closes are recorded as they come, and non-US instruments' sessions are not considered
(#63). Phase order **eligibility → what-if → rates**. SOURCE lane `etoro_crowd` (no new lane; serialises with
the 21:52 crowd and 22:07 investor recorders; a nominal ~45 min run ends long before 21:52). `catch_up_on_boot`
+ `rearm_on_lost_fire`: a late run is honestly stamped; a lost day is unrecoverable. `row_count` = eligibility
rows + rate rows + what-if `ok` rows (#85). Read-only: eligibility and what-if are informational
(`get_what_if_costs` docstring), no order path is touched.

**Quota** (#67-#74). Our own clients pace each lane under its documented limit (broker write pacing 3.5 s for B
and C; market-data 1.1 s for F). They do NOT reserve the quota: other callers of B (`core_eligibility_refresh`
hourly at :20, `strategy_position_manager`), C (the executors and preflight) and F (candles, crowd search,
`quotes_refresh` at :23) hold their own per-instance clocks. 19:07 puts eligibility (≈ 129 requests ≈ 7.5 min
of pacing) before the :20 refresh on a normal day; nothing guarantees it (retries, latency, boot catch-up).
The mitigations are the bounded request count (≈ 129 + ≤ 500 + ≈ 257 logical requests; up to 4× that in
attempts with `ResilientClient`'s 3 retries), its 429 retry, and `MAX_CONSECUTIVE_ERRORS`. There is no
wall-clock deadline (#74).

## Acceptance

Unit/db tests cover every failure and envelope rule above (#82). Live: three consecutive UTC days each with a
`complete` snapshot and a `success` run, AND the reconciliations below holding per snapshot (#83/#84):
```sql
select (started_at at time zone 'UTC')::date as d, snapshot_id, status, universe_size,
       eligibility_ok, eligibility_expected, whatif_ok, whatif_expected, rates_ok, rates_expected
from etoro_perishable_snapshots order by snapshot_id;
-- eligibility: found + not_found + omitted = universe, omitted expected 0
select s.snapshot_id, s.universe_size, count(e.instrument_id) filter (where e.answer = 'found') as found,
       count(e.instrument_id) filter (where e.answer = 'not_found') as not_found,
       count(e.instrument_id) filter (where e.short_x1 = 'available') as shortable
from etoro_perishable_snapshots s left join etoro_eligibility_observations e using (snapshot_id) group by 1, 2;
-- rates: requested ids with no rate row, reported as a count (a provider omission, not a failure)
select r.snapshot_id, count(*) from etoro_perishable_requests r cross join unnest(r.instrument_ids) as i(id)
where r.phase = 'rates' and r.outcome = 'ok' and not exists (select 1 from etoro_rate_observations o
  where o.snapshot_id = r.snapshot_id and o.instrument_id = i.id) group by 1;
select status, started_at, row_count from job_runs where job_name = 'etoro_perishables_snapshot' order by started_at;
```

Security: read-only market-data and informational preflight endpoints on the demo account; no broker mutation.
