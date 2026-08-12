# Completed-session candle population watermark

Issue: #2572

## Finding

`daily_candle_refresh` commits each instrument independently so a restart does
not discard several hours of valid work. A later failed/orphaned run can
therefore leave a small population on a newer `price_date`. On 2026-08-12 the
newest date existed for 551 of the 4,351-name current US research cohort while
the prior provider session existed for 3,508. SPY and raw `MAX(price_date)` both
named the thin date, so neither proved population completion.

The refresh also runs at 03:00 UTC. On a Tuesday that is 23:00 New York on
Monday: Monday is complete and Tuesday has not opened. The former weekday-only
freshness rule nevertheless named Tuesday. A manual/pre-open retry could fetch
and publish a forming Tuesday candle as though it were a close.

## Contract

The run freezes `latest_completed_us_session(now)` once. The resolver uses the
NYSE holiday calendar and official 16:00 ET regular / 13:00 ET half-day close.
That same date controls T3 scope, per-instrument freshness and the maximum daily
candle allowed into `price_daily` for the run. A provider response beyond the
boundary remains in the provider's bounded raw audit path but is not published
as a completed close.

Before the long sweep, the existing `job_runs.progress_json` row is checkpointed
with:

- contract/source/scope versions and a hash of the ordered instrument IDs;
- completed provider session and `population_status=running`;
- declared count and zeroed attempted/successful/usable/unavailable/failed
  counters.

After the sweep, one aggregate coverage query finalises those counters. A
worker orphan leaves the initial checkpoint beside the failed run, making the
partial state explicit. There is no new table, migration, per-instrument
telemetry or derived-series retention.

`unavailable` is `declared - usable`; it may overlap a failed attempt because
the usable measure is the database state at the terminal boundary, while
failed describes this run's operation. `successful` counts clean attempted
fetches and excludes freshness skips. These axes must not be summed.

## Consumer audit

| Consumer | Partial-newest-date safety | Disposition |
|---|---|---|
| candle T3 scope and per-instrument skip | unsafe before this change | both now share the completed-session boundary |
| process candle watermark | unsafe global maximum | reads the aggregate checkpoint; legacy runs retain a MAX fallback |
| orchestrator T1/T2 content gates | per-instrument, but wrong pre-open civil date | now require the last completed US session |
| strategy signal scan | safe | already uses a modal frontier plus a frozen coverage floor |
| completed-session regime context | safe | already selects the latest reference session meeting declared cohort coverage and preserves intervening sparse dates |
| thesis outcomes | safe for its single-instrument maturity question | per-instrument maximum; a newer valid/partial bar merely advances that instrument |
| market-data supply/freshness helpers | safe with explicit boundary | maxima are per instrument, and scheduled refresh now supplies the boundary |
| scoring freshness | operational liveness only, but global newest can cause an early rerun | follow-up: compare against the declared session rather than raw MAX |
| fair-value batch anchor | unsafe | follow-up: replace global MAX with a declared population frontier before the next batch materialisation |
| legacy ops `prices` age | unsafe as a population/completion claim | treat as liveness-only until switched to aggregate watermark |

Portfolio/T1-T2 freshness and broad research completeness remain different
questions. This run watermark describes the resolved refresh scope; a strategy
must still declare its own point-in-time cohort and coverage floor. It may not
interpret a successful job, SPY presence or the aggregate full-scope ratio as
evidence that its research population is complete.

## Verification

Fixtures cover 03:00 UTC, regular close, half-day close, timezone refusal,
forming-bar exclusion, initial checkpoint ordering, exact terminal counters,
failed-attempt accounting and the process-watermark preference over a raw
partial maximum.
