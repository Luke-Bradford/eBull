# #3381 slice 2 — top-investor cohort recorder

Status: proposal (2026-09-25). Parent: #3381 (slice 1 = per-instrument crowd snapshot, merged `3842a878`).
Programme: `docs/proposals/ta/2026-09-25-pattern-hunt-programme.md` build step 1 ("raw payloads, request
parameters, cohort membership, collection failures"; investors who go private are kept as unavailable).

## Why

`.claude/skills/data-sources/etoro-api.md` (§"OPTIONS … MEASURED 2026-09-25", last paragraph): the top popular
investors probed returned empty per-instrument `exposureItems` from `exposure/history`, so we hold no source
of an investor's past positions. The recording clock is the dataset. Family 7 of the hunt ("eToro crowd
extremes") is evaluable only after ≥ 12 months of this record.

## Source rule (live portal read 2026-09-25 + the committed OpenAPI `v1.375.0` fixture)

| endpoint | quota (`x-ratelimit` in `tests/fixtures/etoro/openapi_v1.375.0.json`) | our lane |
| --- | --- | --- |
| `GET /api/v2/portfolios/rankings` (`getPortfoliosRankings`) | 60/60 s, `shared: true, defaultPool: true` | **G_default_shared** |
| `GET /api/v1/user-info/people/{username}/portfolio/live` (`getUserInfoPeopleByUsernamePortfolioLive`) | 60/60 s, `shared: false, defaultPool: false` | new **H_user_live_portfolio** (dedicated; env-neutral: the path has no env segment) |

Rankings params: `period` REQUIRED (enum incl. `LastTwoYears`), `sort` (camelCase, `-` = desc), `page` ≥ 1,
`pageSize` ≤ 100, `popularInvestor` bool. Response `{results: [RankItem], pagination: {page, pageSize,
totalItems, hasNext}}`; `RankItem` carries `cid` (public customer id), `username`, `copiers`, `riskScore`,
`gain` (decimal fraction), ~45 more metrics. Live portfolio: `{realizedCreditPct, unrealizedCreditPct,
positions: [Position], socialTrades: [{…, positions: [Position]}]}`; documented errors: 429 only; no
pagination is documented.

Observed once (2026-09-25, demo key, read-only; not a population claim): the ranking reported `totalItems`
579 at 100 rows/page; an unknown username answered 404; ranks 1–3 answered 200 with 180/223/183 top-level
positions and 0 `socialTrades`. A private profile's status was NOT observed.

## What the cohort is — and is not

A popularity-selected sample BY DESIGN: the investors the eToro crowd follows most. It is not a sample of
investors at large; public-profile and PI-programme filters act before entry, and no rule here can undo that
(Codex ckpt-1 #3/#5/#15). What the design does prevent is attrition AFTER entry.

## Cohort rule (fixed by construction — no published rule exists)

`COHORT_RULE_VERSION = "pi-lt2y-copiers-top100-sticky-v1"` names the SELECTION constants only:

1. **Ranking census:** walk `period=LastTwoYears, popularInvestor=true, sort=username, pageSize=100` until
   `hasNext` is false; record every row and every page's `pagination` envelope. ⚠ Sorted by `username`, a
   unique key, NOT by copiers — measured 2026-09-25: paging `sort=-copiers` repeated a copiers-tied row
   across the page 4/5 boundary (so skipped another) on 7 of 11 walks; multi-field sorts (`-copiers,cid`)
   and `sort=cid` were far worse (as few as 397 distinct of 579); `sort=username` paged cleanly 9 of 9. A
   walk that repeats a `cid`, changes `totalItems` or comes back short is re-walked, up to
   `RANKING_ATTEMPTS = 3` (the attempts are recorded); a malformed page is not re-walked.
2. **Selection:** rank locally over the complete census by (`copiers` desc, missing last; `cid` asc) — a
   total order, so ties cannot move a member and selection is reproducible from the stored rows — and take
   the first `COHORT_TOP_N = 100` (fewer rows → all of them; ZERO rows → the run refuses).
3. **Sticky membership:** the fetch set = today's selection ∪ every `cid` in the membership ledger under ANY
   rule version. Minting a new version never stops observing an old member (#9–#11). Keyed by `cid`.
4. **Username:** today's ranking username for the `cid` when present (`username_from_ranking = true`),
   otherwise the most recent username recorded for that `cid` in a fetch row (by `observed_at`). ⚠ The live
   response carries no `cid`, so a username reassigned to another account would be undetectable; the flag
   makes every such fetch identifiable (#24/#25).
5. **Fetch order:** today's local rank, then `cid` for members absent from today's ranking.

Operational constants (NOT part of the version, #10): pacing 1.1 s per lane (> the rolling-window floor
`min_interval_for_stamps(60, 60)`); `MAX_TRACKED = 1000`, sized so a full fetch pass fits in ~20 minutes at
that pacing (1000 × 1.1 s ≈ 18.3 min) — above it the run refuses before fetching, but still commits the
ranking and membership (below), so evidence is never lost and the refusal is loud; `MAX_CONSECUTIVE_ERRORS
= 10` members in a row erroring aborts the run (bounds quota spent against a systemic failure, #50);
`MAX_RANKING_PAGES = 50`.

## Schema (`sql/425_etoro_investor_cohort.sql`) — UPDATE refused by trigger on every table (as sql/424)

- `etoro_investor_snapshots` — one row per attempt: `status ∈ {complete, partial, failed}`,
  `cohort_rule_version`, `request_params` (incl. `recorder_version`), `ranking_envelopes` (every page's
  `pagination`), `ranking_attempts`, ranking counters, `investors_expected` (fetch-set size), `attempted`, `fetched`,
  `unavailable`, `errored`, `error`. Invariants: `fetched + unavailable + errored = attempted ≤ expected`;
  `complete` ⇔ not failed, `attempted = expected`, `errored = 0`; `partial` ⇔ not failed, `attempted =
  expected`, `errored > 0`; `failed` ⇔ `error` set.
- `etoro_investor_rankings` — every ranking row: `(snapshot_id, rank)` PK (the local rank above),
  `UNIQUE (snapshot_id, cid)`,
  `username, observed_at, copiers, risk_score, gain, raw`.
- `etoro_investor_cohort` — membership ledger `(cohort_rule_version, cid)` PK, `first_snapshot_id,
  first_selected_at` (= `observed_at` of the ranking page that carried the row). Inserted with `ON CONFLICT
  DO NOTHING`; never updated.
- `etoro_investor_fetches` — one row per attempted member: `(snapshot_id, cid)` PK, `username,
  username_from_ranking, observed_at, selected_today, outcome ∈ {ok, unavailable, error}, http_status (NULL
  only for a transport error), realized/unrealized_credit_pct, position_count (= len(top-level positions)),
  social_trade_count (= len(socialTrades)), raw` (body as served; JSON string for text; the exception text for
  a transport error).
- `etoro_investor_positions` — parsed top-level `positions` of an `ok` fetch, FK to the fetch row. Published
  units, unconverted. ⚠ Copied (social-trade) positions live only in the raw body — top-level rows are NOT
  total exposure for an account that copies (#65). `open_timestamp` is the position's open time, never a
  knowledge time (#34).

**Clocks.** A row's `observed_at` = receipt of the response it came from (after the client's retries); for
a transport error, the time the error surfaced. A snapshot's knowledge time as a whole is `finished_at`.
Daily snapshots cannot see positions opened and closed between runs (#40).

## Failure semantics

- **Ranking failure** (non-2xx after retries, invalid/missing pagination, `page` mismatch, a row without
  `cid`/`username`, zero rows, > `MAX_RANKING_PAGES`; or `totalItems` changing, a repeated `cid` or rows ≠
  `totalItems` on every one of the `RANKING_ATTEMPTS` walks) → `failed` header only.
- **After the ranking completed**, a run that aborts (401 on any member, `MAX_TRACKED` refusal,
  `MAX_CONSECUTIVE_ERRORS`, any other exception) commits a `failed` header TOGETHER with the ranking rows, new
  membership and every fetch completed so far (#14/#43). If that write itself fails, a bare `failed` header is
  attempted best-effort. The original exception is re-raised (slice 1's pattern, so `classify_exception`
  sees a 401/429).
- **Per member:** 403/404 → `unavailable`; a malformed 2xx body → `error` for that member only, raw kept; any
  other status or exhausted retries/transport error → `error`.
- A run with `attempted > 0` and `fetched = 0` is a systemic failure (#51): its snapshot is recorded as
  `failed` with all rows, and the run raises.
- A `partial` snapshot is committed, THEN the run raises `InvestorSnapshotPartial` outside the failure
  handler (no second header, #48), so `job_runs` never reports `success` for lost members.
- A hard crash can leave no row at all; the next scheduled or catch-up fire is the recovery (#42).

## Job

`etoro_investor_snapshot`, daily **22:07 UTC**: sequenced after the 21:52 crowd snapshot, after the US close in
both DST regimes, off the 5-minute grid. SOURCE lane **`etoro_crowd`** (reused per the issue: no new lane; the
lane serialises the two recorders, so a late crowd run delays this one rather than overlapping it).
`catch_up_on_boot` + `rearm_on_lost_fire` (a late snapshot is honestly stamped; a lost day is unrecoverable).
Read-only. Provider `EtoroSocialProvider` (`app/providers/implementations/etoro_social.py`), one
`ResilientClient` per lane. ⚠ Each client's clock is per-instance; lane G is also used by other default-pool
readers, so aggregate lane-G compliance rests on the small call count (#72) — same standing as every other
lane-G reader today.

## Volume and runtime

One end-to-end run against the live API (2026-09-25, into a scratch database on the test cluster; script
not committed): `complete`, 1 ranking attempt, 6 pages, 579/579 ranking rows, 100/100 members `ok`, 16,241
positions over 1,263 distinct instruments (298 short, 138 with leverage > 1), 0 `socialTrades`; on disk
fetches 1,152 kB + positions 3,840 kB + rankings 1,368 kB (`pg_total_relation_size`); 254 s wall-clock.
⚠ Two live-portfolio calls drew a 429 (HTML body, `Retry-After: 60`) at the documented 1.1 s pacing and
succeeded on retry. The sticky set grows with churn; re-measure size and runtime from the stored rows after
the first week.

## Acceptance

Three consecutive UTC days, each with a `complete` snapshot (`fetched > 0` is implied) and a `success` job run:
```sql
select (started_at at time zone 'UTC')::date as utc_day, status, ranking_total_items, ranking_recorded,
       investors_expected, investors_attempted, investors_fetched, investors_unavailable, investors_errored
from etoro_investor_snapshots order by snapshot_id;
select status, started_at, row_count from job_runs where job_name = 'etoro_investor_snapshot' order by started_at;
select cohort_rule_version, count(*) from etoro_investor_cohort group by 1;
```
Expected: `ranking_recorded = ranking_total_items`, `attempted = expected`, and `expected` ≥ 100 (the
selection plus any dropped-out members).

Security: read-only public-profile endpoints; no broker mutation. Each cohort member costs one request per
snapshot plus `ResilientClient`'s bounded retries on 429/5xx; unavailable members stay in the fetch set by
design (they are the survivorship evidence).
