# Tax FX for the UK ledger: Bank of England XUDLUSS (#3616 slice 3b)

Source: #3616 (committee review, 2026-10-04): *"GBP after-tax P&L against the UCITS/ISA comparator."* Slice 2
(`docs/specs/metrics/2026-10-12-3616-gbp-isa-comparator.md`) decided that an after-tax figure must come from disposals
matched under HMRC rules in `tax_ledger`. Every trade on this account is in US dollars, so every acquisition and
disposal needs a sterling rate for its date. `tax_ledger` has no source for one.

Slice 3 was first specified as one change (commit `9d66ddad` on this branch). Codex checkpoint 1 round 1 returned 18
BLOCKING findings, and the slice is now split:
- **3a**, the s105 matcher fix, merged in #3789 (`c4270923`).
- **3b**, this spec: the tax FX source. It adds new tables and a job, and changes no existing tax result.
- **3c**: the account's broker trades as tax evidence (`trade_events` → `tax_lots`). 3c switches `_load_fx_rate` to
  this slice's resolver and retires the unused `fx_rates` table.
- **3d**: tax character, matching from broker evidence, and publication on `/tax`.

The round 1 findings that 3c and 3d must apply are listed on #3616.

## Facts established first

Each fact names its evidence.

**The measurement.** `scripts/measure_3616_boe_xudluss.py` reads a full-series download and the GOV.UK calendar, and
writes `docs/research/3616-slice3b-boe-xudluss-2026-10-13.json`. The output holds counts, dates and percentages,
never a rate.

**The probes.** `scripts/probe_3616_boe_endpoint.py` writes `docs/research/3616-slice3b-inputs/probes-2026-10-13.json`.

**What is committed and what is not.**
- The calendar input is committed in `docs/research/3616-slice3b-inputs/`, because GOV.UK content is under the Open
  Government Licence v3.0.
- The Bank's bodies are kept off the public repository (fact 8). They live on the dev machine under
  `var/research/3616/inputs/` (gitignored), named `<probe>-<sha256 prefix>.body`, plus the full-series download
  `XUDLUSS_1975-01-01_2026-10-13.csv.gz`. Each is identified by its full sha256 in the outputs.
- A later download can differ, because the Bank may revise. Reproducing the facts exactly therefore needs those
  private files.

1. **The tax `fx_rates` table (sql/013) has no writer** (code inspection).
   - `grep -rnE "(INSERT INTO|UPDATE|COPY) +fx_rates\b" app scripts sql tests` returns nothing.
   - `tax_ledger._load_fx_rate` (`app/services/tax_ledger.py:252-285`) returns 1 for GBP and GBX. For any other
     currency it returns the stored row, and raises when there is none.
2. **Dev DB, 2026-10-13: `fx_rates` and `tax_lots` both hold 0 rows** (query).
3. **Settled decision.** "Own EOD NAV-snapshot table (#1594)" keeps the ECB `fx_rates_daily` apart from the tax rates.
   This slice touches neither table.
4. **The XUDLUSS series, 1975-01-01 to 2026-10-13** (measurement; body sha256 `6e87fd5c6b17…`).
   - It holds 13,089 rows, 1975-01-02 to 2026-10-12. The body is ASCII with CRLF line ends, and the parser contract
     (Design 2) accepts every row.
   - Values have at most 4 decimal places; trailing zeros are dropped.
   - The largest move between consecutive observations, |ln(b/a)|, is 8.29% (2016-06-23 → 06-24). Since 2019 it is
     3.32%.
5. **The calendar and the series agree exactly** (measurement, cross-check; calendar sha256 `538b3482…`).
   - GOV.UK `bank-holidays.json`, England and Wales, lists 2019 to 2028 with 8 to 10 dates a year.
   - Over every date from 2019-01-01 to 2026-10-12, the observed dates are exactly the working days: Monday to Friday,
     not a bank holiday.
   - There are 0 exceptions in either direction, weekends included, across 65 weekday holidays.
6. **Endpoint behaviour** (probes; `Accept-Encoding: identity`, redirects not followed).
   - The full range with `VPD=N`, with `VPD=Y` and with no `VPD` returned byte-identical bodies. `VFD=N` was held
     constant.
   - The full-range response carries no `Content-Length`. The supported range, 2019-01-01 to 2026-10-13, carries
     `Content-Length` 39,113, equal to its body, which is 9,010 bytes gzipped. Neither carries a `Content-Encoding`.
   - A weekend-only range returns 200 and the header line only.
   - An unknown series returns **302** with `content-type: application/csv` and an HTML body.
   - **The User-Agent strings `Python-urllib/3.x` and `python-httpx/0.28.1` (httpx's default) get 403 "Access
     Denied".** The build's identifying string, `eBull/1.0 (+https://github.com/Luke-Bradford/eBull)`, gets 200.
     The probes sent each string through urllib; the build tests its real httpx client against the same rule.
7. **Bank of England statements.**
   - **Statistics FAQ.** *"all published Interest & Exchange rates data is published at 9:30am every working day"*.
     Permission requests for these data go to the Bank's data team.
   - **Database help, authority.** *"The exchange rates are not official rates and are no more authoritative than
     that of any commercial bank operating in the London foreign exchange market."*
   - **Database help, timing.** *"We aim to update the Database by 10am. Daily rates, depending on the series will
     publish 1 -2 working days after the date to which they relate. At times however, there may be further
     delays."*
   - **Database help, provisional data.** `VPD=Y` *"need only be included if provisional data is desired"*.
   - **Revisions.** No page states a revision schedule or a finality guarantee. The help documents a view of *"new and
     revised data"*. The legal page reserves the Bank's right to change information, timing and methods without
     notice.
   - **Spot-rate notes.** The rates are *"prepared using multiple sources"*, possibly including LSEG, whose content
     may not be republished or redistributed without LSEG's consent.
8. **Reuse terms.**
   - The Bank's legal page permits users to *"download, display or print the Resources for personal use or internal
     use"*.
   - Reproduction of database data falls under the Open Government Licence, which excludes *"selected exchange rate
     data and series"*.
   - **The eBull repository is public** (`gh repo view`: `PUBLIC`).

## Source rule

HMRC CG78310: *"an amount of foreign currency must be converted into its sterling value at the time the amount is
incurred/received. HMRC does not prescribe what reference point should be used for the exchange rate. It is, however,
expected that a reasonable and consistent method is used."* The worked example converts *"at the exchange rate in force
at the date of acquisition"* and *"at the date of disposal"*.

CG78310 rules out computing a gain in dollars and converting it, so each amount is converted at its own date. That
conversion is 3c's; this slice supplies the rate for a date.

No HMRC rule names a rate source or a non-publication-day rule. Every choice below is this project's judgement of a
reasonable and consistent method, frozen under `TAX_FX_POLICY_ID` ("Local policy").

**Reuse.** This project's use is personal and internal: one investor's own tax computation, inside a single-operator
authenticated app.
- **Nothing that holds a Bank rate enters the public repository.** That covers bodies, fixtures and quoted values.
  Tests use synthetic values.
- **No endpoint in this slice exposes a rate.**
- **Any later display or export of raw rates beyond the operator needs permission first,** from the Bank and from any
  third party whose content the rates include. That is a constraint on 3d.
- **One question stays open.** The published pages do not settle whether persistent private storage of XUDLUSS falls
  under the legal page's personal and internal-use permission, or under the database terms.
  - **Recommendation:** treat it as permitted personal and internal use, and confirm with the Bank's data team before
    any use beyond that.
  - **Safe default if the answer is no:** drop `body_gz` retention and keep only hashes.

## Design

### 1. Storage (migration `sql/484_tax_fx_boe.sql`)

- **`tax_fx_runs`**: one row per job run, created first.
  - Columns:
    - `run_id`, an identity assigned under the run lock (Design 2), which totally orders runs;
    - `started_at` and `finished_at`;
    - `state` (`in_progress`, `applied`, `refused`, `skipped` or `abandoned`) and `reason`;
    - `eligible`, true when every check except confirmation (Design 2, steps 4.1–4.9) passed;
    - `policy_id` and `parser_version`;
    - `calendar_used` (`fetched` or `stored`);
    - `latest_observation_date`;
    - the counts `inserted`, `revised`, `awaiting_confirmation` and `calendar_years_changed`.
  - `applied` is written in the same transaction as the derived changes it made.
- **`tax_fx_fetches`** is the raw evidence: one row per download attempt. Rows are never updated or deleted.
  - Columns:
    - `fetch_id` and `run_id` (FK);
    - `source` (`boe_xudluss` or `govuk_bank_holidays`), `request_url`, `user_agent`, `fetched_at`;
    - `outcome` (`received`, `incomplete` or `transport_error`) and `error`;
    - `http_status`, `content_length_header` and `content_encoding_header`, each NULL when absent;
    - `body_gz` (the exact entity bytes received, gzipped), `body_sha256` and `body_length`.
  - `incomplete` means headers arrived and the transfer then failed. Any bytes received are kept.
  - Retention is permanent. At the measured 9,010 gzipped bytes, the BoE body takes about 3.3 MB in the first year;
    it grows slowly as the range lengthens. Calendar bodies and database overhead are extra.
- **`tax_fx_observations`** holds the current value per date: `observation_date` (PK), `usd_per_gbp NUMERIC(12,6)`
  and `set_by_run_id`.
- **`tax_fx_revisions`** records every value change: `observation_date`, `old_usd_per_gbp`, `new_usd_per_gbp` and
  `run_id`. Rows are append-only.
- **`tax_fx_calendar_years`** holds the England-and-Wales holidays for each year.
  - Columns: `year` (PK), `holiday_dates date[]` and `source` (`seed:<sha256>`, or `run:<run_id>` for the run that
    last added or changed it).
  - The migration seeds 2019–2028 as literal rows from the committed calendar file.
  - The constant `CALENDAR_SEED_SHA256` is that file's full sha256. A test asserts the migration's rows equal the
    file.
- **`tax_fx_calendar_revisions`** records every added or changed year: `year`, `old_dates` (NULL when added),
  `new_dates` and `run_id`. Rows are append-only.

### 2. A run

A run holds the exclusive session advisory lock `pg_advisory_lock(TAX_FX_LOCK_KEY)` on its own dedicated connection
from start to end. A second invocation of any kind, scheduled or manual, waits, so runs never overlap.
- The lock is released with `pg_advisory_unlock` in a `finally`.
- If the unlock cannot be confirmed, the connection is closed rather than returned to a pool.
- Publishers take the same key shared (Design 3).

1. **Open the run.** Mark any earlier `in_progress` run `abandoned`. Insert this run as `in_progress` and commit.
2. **Fetch.** Fetch both sources and commit each fetch row at once, linked to the run.
   - Both requests send `User-Agent: eBull/1.0 (+https://github.com/Luke-Bradford/eBull)` (fact 6) and
     `Accept-Encoding: identity`. They time out after 30 s and never follow redirects.
   - **BoE.** The CSV URL from the measurement script, with `VPD=N` (fact 7: no provisional data) and `VFD=N`.
     - `Datefrom` is `FX_FLOOR = 2019-01-01`, the calendar seed's first year.
     - `Dateto` is today in Europe/London.
     - Dates use a fixed English month table, never the process locale.
   - **GOV.UK.** `https://www.gov.uk/bank-holidays.json`.
3. **A BoE fetch that is not `received`** makes the run `skipped`, and the job raises. It adds no evidence, so the
   current state stands.
4. **Otherwise one transaction validates and applies.** The first failed check rolls the transaction back. The run is
   then committed as `refused`, with the check and the dates involved, and the job raises.
   1. **BoE response.** HTTP 200; no `Content-Encoding`; `body_length` equals `Content-Length` when that header is
      present; and the body passes `parse_xudluss_csv`. An empty observation list is refused.
   2. **Calendar response.** A received calendar needs HTTP 200, the same length rule and `parse_bank_holidays`.
      - A calendar that is not received means `calendar_used = 'stored'`.
      - A received calendar that fails any of these refuses the run.
   3. **Plausibility.** Every move between consecutive observations in the body is at most 15% |ln|. That is about
      1.8 times the largest move since 1975 (fact 4).
      - Every value that differs from its stored value is also within 15% of it.
      - At most 5 stored dates differ in one run.
      - These are anomaly gates against a misplaced decimal point or a rescaled series, not proof of correctness.
   4. **No regression.** Every date in `tax_fx_observations` is in the body. A missing date cannot be told apart from
      a truncated response, so it refuses.
   5. **Calendar changes.** A received calendar may add years. It may change a stored year only for dates after today
      (Europe/London).
      - Any change to today or earlier refuses as `calendar_history_changed`. That covers a date between the old and
        new *L*, because *L* is never after today.
      - The window is "after today", not a longer notice period, because special holidays can be declared at short
        notice. The 2022 state funeral holiday, 2022-09-19, is in the calendar.
      - The seed's dates up to 2026-10-12, the measured-through date, are therefore fixed. A date can change only while
        it is still in the future.
      - A false future holiday is caught when that date passes. Step 4.7 refuses any observation on it, and a matching
        missing observation must survive two consecutive eligible bodies (step 4.10).
   6. **Continuity.** The calendar covers every year from `FX_FLOOR` through next year.
   7. **Exact coverage.** Over every date from `FX_FLOOR` to the body's last observation, the observed dates equal the
      working days under the candidate calendar, in both directions. This refuses a suffix-only or interior-gapped
      body, and an observation on a weekend or holiday.
   8. **Freshness.** The body's last observation is no more than `BOOTSTRAP_MAX_LAG = 3` working days before today.
      When `tax_fx_observations` is empty, a stale body refuses as `bootstrap_stale`.
   9. **Eligibility.** A run that reaches this point is `eligible`.
   10. **Confirmation, the last check.** A value is *confirmed* when the previous run with a received BoE fetch was
       `eligible` under the same `policy_id` and its stored body holds the same value for that date.
       - A changed stored value that is unconfirmed refuses the run as `revision_unconfirmed`. The run stays
         `eligible`, so the next run can confirm it.
       - An unconfirmed new date waits, counted in `awaiting_confirmation`. It is not applied, and it resolves as
         `pending`.
       - So every stored value has been seen in two consecutive eligible responses. A one-off bad response, bootstrap
         included, never becomes a rate.
   11. **Apply.** Insert confirmed new dates. Update confirmed revisions, writing a `tax_fx_revisions` row for each.
       Write the calendar candidate's added and changed years, with a `tax_fx_calendar_revisions` row for each. Mark
       the run `applied` with its counts.
5. **After commit, the job raises `feed_stale`** if the body's last observation is more than `STALE_ALERT_LAG = 3`
   working days old: the Bank's two-working-day window plus one. That alert is operational. `BOOTSTRAP_MAX_LAG` is
   policy.

**Parser contracts.** Both move from the measurement script into `app/providers/implementations/boe.py` and
`govuk_calendar.py`.
- **`parse_xudluss_csv`**:
  - the body is ASCII and CRLF-terminated, with the exact header `DATE,XUDLUSS`;
  - each row matches `DD Mon YYYY,<1-6 digits>[.<1-6 digits>]`, with an English month token and a real date;
  - the value is above zero, which fits `NUMERIC(12,6)`;
  - dates strictly increase within the requested interval.
  - Whitespace, a sign, an exponent, an underscore, another month spelling, LF-only line ends and HTML all refuse.
- **`parse_bank_holidays`**:
  - the `england-and-wales` division is present, with unique ISO dates;
  - each listed year from the first to the last holds at least 8 dates.
  - The count is a plausibility check only. Agreement with the series comes from steps 4.5 and 4.7.

**Replay.** The derived tables can be rebuilt from the seed file (by `CALENDAR_SEED_SHA256`), `tax_fx_runs` and
`tax_fx_fetches` alone.
- Start from an empty store and the seed calendar.
- Re-run steps 4.5 and 4.10–4.11 for each `applied` run in `run_id` order, over its stored BoE and calendar bodies.
- Confirmation reads the stored body of the previous received run, whatever that run's state, so refused-eligible
  runs take part as confirmers.
- The planning function is the same code the job uses.
- A test rebuilds both from evidence after a sequence that includes a revision and a changed future calendar year, and
  compares the result with the live tables.

**A crash** at any point leaves a run `in_progress` that the next run marks `abandoned`. If that run had a received BoE
fetch, the source is unhealthy until a later run applies (Design 3).

### 3. Which rate a date takes

`load_tax_fx_state(conn) -> TaxFxState` reads the health, *L*, the observations, the calendar and the latest applied
`run_id` in one `snapshot_read` (REPEATABLE READ) block.
- `snapshot_read` commits any pending transaction on entry (`app/db/snapshot.py`). The function therefore raises
  unless the connection is idle.
- 3c loads the state before it opens any write transaction, or on a separate connection.

`resolve_tax_fx(state, rate_date) -> TaxFxRate` is pure. 3c passes each event's Europe/London date, as `_to_uk_date`
does today.

**Health.** The source is healthy when three conditions hold:
- the latest run with a `received` BoE fetch is `applied`;
- that run's `policy_id` equals `TAX_FX_POLICY_ID`;
- `tax_fx_observations` is not empty.

An `in_progress`, `abandoned` or `refused` run with a received body therefore makes the source unhealthy. A `skipped`
run does not count. Bootstrap takes two runs: the first is eligible but confirms nothing.

The statuses, in order:
1. **`unavailable`.** The source is not healthy. Every date refuses.
2. **`out_of_range`.** The date is before `FX_FLOOR`.
3. **`pending`.** The date is after *L*: it is not in accepted evidence, whether unpublished, delayed or truncated.
   The calendar is never consulted after *L*.
4. **`published`.** The date has an observation. The rate is its value.
5. **`carried_forward`.** Otherwise the date is a weekend or holiday on or before *L*, because step 4.7 verified every
   working day up to *L*. Let *p* be the latest earlier observation date.
   - The rate is *p*'s value.
   - If no observation precedes the date on or after `FX_FLOOR`, as for 2019-01-01, the answer is `out_of_range`.
   - A working day on or before *L* with no observation cannot occur in a healthy state. If one is met, the resolver
     raises an invariant error and never returns a rate.

`TaxFxRate` carries:
- `rate_date` and `status`;
- `observation_date` and `usd_per_gbp`;
- `gbp_per_usd`, which is 1 / `usd_per_gbp` rounded half-even to 10 places, the scale of `tax_lots.fx_rate_to_gbp`;
- `run_id`, the applied run whose evidence produced the state;
- `policy_id`.

The rate fields are NULL unless the status is `published` or `carried_forward`.

Rounding moves `gbp_per_usd` by at most 5×10⁻¹¹, so a converted amount moves by at most the dollar amount × 5×10⁻¹¹:
£0.0005 on $10,000,000.

`tax_fx_state_is_current(conn, state) -> bool` takes `pg_advisory_xact_lock_shared(TAX_FX_LOCK_KEY)`. It then reads,
at READ COMMITTED, whether the source is still healthy and the latest run with a received BoE fetch is still
`state.run_id`.
- The shared lock conflicts with a run's exclusive lock and is held to the caller's commit, so no run can change the
  answer between the check and the publication.
- It raises if called inside a REPEATABLE READ or stricter transaction, whose snapshot could be older than the check.

**The consumer contract, which 3c and 3d must follow.**
- **Stored on each lot:** `rate_date`, `observation_date`, `usd_per_gbp`, `policy_id` and `run_id`. The `run_id` is
  provenance only.
- **When a lot is stale:** `resolve_tax_fx(rate_date)` on a current state returns a different `observation_date`,
  `usd_per_gbp` or `policy_id`. That comparison covers a revised value, a newly published date that was previously
  carried, and a policy change.
- **Publishing:** results are valid as of the state they were computed from. A publishing transaction, at READ
  COMMITTED, calls `tax_fx_state_is_current` before its writes and aborts if it is false.
- **While `unavailable`:** nothing FX-dependent is published, and staleness is decided once the source is healthy
  again.

`TAX_FX_POLICY_ID = "boe_xudluss_ew_calendar_v1"` names:
- the request's data parameters: `VPD=N` and `FX_FLOOR`;
- the parser contracts;
- validation steps 4.1–4.10 and `BOOTSTRAP_MAX_LAG`;
- the resolver rules.

Changing any of them is a new policy ID, so every dependent lot goes stale. The schedule, the User-Agent and
`STALE_ALERT_LAG` are operational and outside the policy ID.

### 4. Job

`tax_fx_refresh` runs daily at 11:43 UTC: 11:43 London in winter, 12:43 in summer, after the Bank's 10:00 update
aim. No job is scheduled at minute 43, and no sub-hourly cadence divides 43. It runs on the existing `db` lane, so no
lane or connection is added. The advisory lock, not the lane, is what guarantees a single writer.

The build registers it in `SCHEDULED_JOBS`, the lane map in `app/jobs/sources.py` and `_INVOKERS`
(`app/jobs/runtime.py`).

The job raises, so `_tracked_job` records `failure`, on any of these:
- a `skipped` run;
- a `refused` run;
- `feed_stale`.

Each run logs its counts, *L* and any revision with its old and new value.

### 5. What this policy cannot absorb

- **Withdrawing a stored observation** fails step 4.4.
- **Publishing on a weekend or holiday** fails step 4.7.
- **Changing a calendar date on or before today** fails step 4.5.
- **A move above 15%, or more than 5 revisions in one run,** fails step 4.3.

Each leaves the source `unavailable` and the job failing daily. The Bank reserves the right to make such changes
without notice (fact 7). The recovery is a new spec that designs the data transition and a new `TAX_FX_POLICY_ID`.
This slice provides no override.

### 6. Not changed

Nothing outside this slice's new tables and job changes:
- `fx_rates` (sql/013), `_load_fx_rate`, `_ingest_fills` and `fx_rates_daily`;
- every tax figure;
- any API or page.

3c is the first consumer.

## Local policy

These are frozen under `TAX_FX_POLICY_ID`. No published rule exists for any of them.

- **XUDLUSS is the reference.** It is a statistical reference series, and the Bank says it is not an official rate
  (fact 7). It is chosen because it comes from the UK central bank, is daily since 1975, needs no account, and is one
  method for every date. That meets CG78310's "reasonable and consistent" expectation in this project's judgement, not
  by HMRC endorsement.
- **A date takes that date's published daily observation**, whatever the time of the event on that date.
- **A non-publication day takes the latest earlier publication.** The England-and-Wales calendar decides
  non-publication days.
  - It is fixed for today and every earlier date.
  - Every run checks that the calendar's date set agrees with the series. That establishes agreement between the two
    accepted date sets, not the calendar's independent truth.
- **A value is used only after two consecutive eligible responses agree on it.** That includes a revision, which then
  replaces the stored value.
- **The anomaly gates are 15% per move or revision, and 5 revisions per run.**
- **Evidence that contradicts the stored state makes the whole source `unavailable`.**
- **`FX_FLOOR` is 2019-01-01.**

## Tests

All rate values in fixtures are synthetic. Bodies are built in the Bank's format with invented rates. No Bank body is
committed (Source rule, "Reuse").

**Parsers** (pure):
- BoE accepts:
  - a valid synthetic body;
  - a header-only body, which parses empty.
- BoE refuses:
  - the committed probe's 302 HTML body, an LF-only body and a wrong header;
  - `5 OCT 2026, +1_000e-3 `, and one case each of whitespace, sign, exponent, underscore and month case;
  - `30 Feb 2026`;
  - the values 0, `1000000` and a 7-place value;
  - a repeated or decreasing date;
  - a date outside the interval;
  - a non-ASCII body.
- Calendar:
  - the committed file is accepted;
  - a missing division is refused;
  - duplicate dates are refused;
  - a year with 7 dates is refused.

**Validate and apply** (a pure planning function over in-memory state, with synthetic bodies whose dates follow the
seed calendar). With an empty store:
- a complete body is eligible but confirms nothing, and the next identical body applies;
- a body scaled ×10 that is followed by the true body confirms nothing;
- a header-only body is refused;
- a suffix-only body is refused;
- an interior-gapped body is refused;
- a body stale beyond three working days is refused as `bootstrap_stale`;
- a body with a 20% jump is refused.

A rate-free probe body (header only, or HTML) is embedded in the manifest. An LF-only rate body and a non-200 CSV body
are not.

With a stored state:
- a missing stored date is refused;
- a changed value is refused once, then applied when the previous eligible run showed the same value;
- a revision confirmed only by a refused-ineligible run (an HTTP 206 body, say) is not confirmed;
- 6 revisions in one run are refused, and so is a revision of more than 15%;
- a new tail date waits one run, then is inserted.

Calendar cases:
- dropping 2019 keeps it;
- removing both 2022 special holidays is refused under step 4.5;
- adding a holiday on a past working day, paired with that date missing from the body, is refused under step 4.5;
- the same pairing on the day after the stored *L* is refused under step 4.5;
- a holiday added for tomorrow applies and is recorded in `tax_fx_calendar_revisions`;
- next year absent from both the stored and fetched calendars is refused under step 4.6.

Transport and framing:
- HTTP 206, 302 and 500 with a parser-valid body are each refused;
- so are a body shorter than its `Content-Length` and a `Content-Encoding: gzip` response.

**HTTP adapter** (httpx `MockTransport`; plus one live check in acceptance that the real client's User-Agent gets
200):
- a failure before headers is `transport_error`;
- a failure mid-body is `incomplete` with its partial bytes kept;
- both make the run `skipped`.

**Resolver** (pure, over synthetic states):
- published;
- a Saturday carried from Friday;
- Good Friday and Easter Monday 2026 carried from 2026-04-02;
- Christmas carried from 24 December;
- 2019-01-01 and 2018-12-31 are `out_of_range`;
- *L* + 1, and a weekend after *L*, are `pending`;
- every date is `unavailable` in each of these states: an `in_progress` run with a received fetch; an abandoned run; a
  refused run after an applied one; an empty store; a policy mismatch;
- a `skipped` run leaves the source healthy;
- `gbp_per_usd` rounds correctly at the extremes of a synthetic range;
- `load_tax_fx_state` raises inside an open transaction.

**Consumer contract** (pure):
- a revised value, a newly published date that was carried, and a policy change each make an earlier answer stale;
- identical evidence does not;
- `tax_fx_state_is_current` is false after a refused run commits.

**DB** (one test, real tables, a synthetic HTTP transport):
- an applied run writes observations, revisions, calendar years and its run row atomically;
- refused and skipped runs leave their fetch and run rows, read back on a fresh connection after the job raises, and
  change no derived row;
- a second run started while the first holds the lock waits;
- after a refusal, a skip and an unexpected exception, another connection can take the lock;
- a refresh trying to commit while a publisher holds the shared lock waits for the publisher's commit;
- a crash simulated after the fetch commit leaves the source `unavailable` until the next applied run;
- a pre-seeded `fx_rates` row, `tax_lots` row and `fx_rates_daily` row are unchanged after a run, and `_load_fx_rate`
  returns the same value for the seeded date.

## Acceptance (dev DB)

After the build merges, the jobs child reloads (PID changed), and `tax_fx_refresh` runs twice (bootstrap, Design 3):
- **Runs.** The first run is `applied` and `eligible`, with `inserted` 0. The second is `applied`, with one `received`
  fetch per source.
- **Observations.** `tax_fx_observations` holds exactly the dates of the second body that the first body confirms, at
  those values. It starts 2019-01-02.
- **Calendar.** `tax_fx_calendar_years` covers 2019 through at least next year.
- **Resolver order.** Each expected status is derived in resolver order: health, `FX_FLOOR`, *L*, observation, then
  calendar. The check covers 2026-10-12, Saturday 2026-10-10, 2026-04-06, 2025-12-25, 2018-12-31 and the run date.
  - Every rate-bearing answer's value equals the stored body's value for its observation date.
  - These values are read on the dev DB and not copied into the repository.
- **Non-interference.** `fx_rates` and `tax_lots` still hold 0 rows, and the build PR's diff does not touch
  `app/services/tax_ledger.py`.

## Full-population verification

The parser contract accepted all 13,089 rows of the full-series download.

The calendar rule matched the series over every date from 2019-01-01 to 2026-10-12, with 0 exceptions in either
direction. That is the complete interval the measured snapshot covers, and it is the historical calendar this policy
freezes.

It is not a proof about later dates or later Bank behaviour, nor about the calendar's truth independently of the
series. Every run therefore repeats the exact-coverage check before applying anything.

## Security

- **Outbound calls.** The job makes two unauthenticated outbound GETs to fixed public URLs, with an identifying
  User-Agent. Their only parameters are dates formatted from `date` objects; no user input reaches a request.
- **Response handling.** Responses are parsed strictly and redirects are not followed.
- **Writes.** The job writes only the six new tables: runs, fetches, observations, revisions, calendar years and
  calendar revisions.
- **No change to any trust boundary.** No endpoint, authorisation check, order path or capital path changes.
- **Data use.** It stays personal and internal. No Bank rate enters the public repository (fact 8).
