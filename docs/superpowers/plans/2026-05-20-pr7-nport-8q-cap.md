# PR7 — N-PORT 8-quarter ingest cap at every writer chokepoint

> Created: **2026-05-20** as the next PR in the #1233 data-retention rubric series.
>
> Tracking issue: **#1233** — Bootstrap scope discipline umbrella.
> Spec: [`docs/superpowers/specs/2026-05-19-data-retention-rubric.md`](../specs/2026-05-19-data-retention-rubric.md) §4.6.
>
> Status: **PLAN** — revised post Codex 1a (2026-05-20). Two BLOCKING +
> three WARN + one LOW addressed inline: §5.7 specifies the native
> ``JobInvoker`` registration migration (BLOCKING 2); §5.2 cleans up
> the contradiction with §5.7 about stage-22 period-floor (WARN 1);
> §5.1 proof tightened to month-end congruence class wording (WARN 2);
> §5.5 reorders bulk gate before CUSIP/series filters (WARN 3); §5.1
> + §7.1 test expectations corrected to the algorithm's true Mar-2024
> → ``2022-03-31`` shape (BLOCKING 1); spec §4.6 amend made explicit
> about the month-end anchor reason (LOW 1).

## 0. Context

The data-retention rubric (#1233) bounds per-source ingest depth so the
clean re-run after the operator pre-wipe (spec §6.3) measures the
post-cap steady state. PR1-PR6 have shipped. PR7 lands the **N-PORT
8-quarter per-fund cap** at every writer chokepoint **and** the
**fund-trust cohort bound** equivalent to #1010 for 13F-HR.

N-PORT is the second-largest non-XBRL ownership volume in the dev DB:
**`ownership_funds_observations`** = 1.6 GB across 3.68M rows;
**`ownership_funds_current`** = 2.5 GB (PR12 audit territory — same
oddity as 13F current). 8-of-~64 partitions survive the cap →
projected post-clean-rerun observations footprint ≈ **0.2 GB** (spec
§8 funds slice = half the institutional projection).

PR7 is the direct mirror of PR6. Differences vs PR6 are:

1. **Month-end anchor, not quarter-end.** N-PORT funds file on their
   own fiscal calendars; `period_of_report` is the END of the third
   month of the fund's fiscal quarter, which can be ANY calendar
   month-end (Jan 31, Feb 28, ... Dec 31). The PR6 quarter-end anchor
   would silently reject fiscal-Q-non-calendar funds. Cutoff anchors
   to **calendar month-ends** instead, admitting 24 calendar months
   = exactly 8 fiscal-quarter snapshots per fund regardless of
   fiscal-year alignment (§5.1).
2. **Fewer chokepoints.** N-PORT has NO `_apply_n_port_*` rewash
   function and NO `sync_funds` repair sweep in
   `ownership_observations_sync.py`. PR7 lint guard enforces only the
   chokepoints that exist (§6 invariants A-F + repo-wide H-I).
3. **Cohort bound is part of PR7.** #1010 added the 13F cohort bound
   BEFORE PR6's depth cap. For N-PORT, the cohort table
   (`sec_nport_filer_directory`, sql/126) already has `last_seen_filed_at`
   + index, so no migration is needed — the filter wiring + bootstrap
   stage 22 dynamic param are added inside PR7 (§5.7).

## 1. Scope

**In**:

1. Month-anchored retention constants + helpers in
   `app/services/n_port_ingest.py`.
2. Cap honoured by every N-PORT writer chokepoint:
   - `parse_submissions_index` (legacy ingester, intrinsic-floor wiring).
   - `_ingest_single_accession` (legacy ingester defensive post-parse gate).
   - `_parse_n_port` manifest-worker adapter (post-parse pre-write
     gate).
   - `ingest_nport_dataset_archive` bulk drain (per-row period gate +
     `rows_skipped_retention` counter on the result dataclass).
3. **Cohort bound** (#1010 mirror for N-PORT):
   - New `list_nport_filer_ciks(conn, *, min_last_seen_filed_at=None)`
     accessor in `app/services/sec_nport_filer_directory.py`.
   - `sec_n_port_ingest` (scheduler) refactored to use the accessor;
     daily standalone path passes `min_last_seen_filed_at=None`
     (full cohort, like 13F daily).
   - Bootstrap-orchestrator stage 22 dispatches the new
     `_PARAM_DYNAMIC_BOOTSTRAP_NPORT_CUTOFF` sentinel resolved to
     `today - 380d` (UTC midnight) at dispatch time.
   - `_resolve_dynamic_params` handles the new sentinel.
   - `ingest_all_fund_filers` signature accepts the param (passed
     through from the scheduler to the directory query at scheduler
     level — no change to the ingester's per-CIK loop).
4. Lint guard `scripts/check_nport_retention.sh` (PR5-style block-level
   placement guard, six placement invariants A-F + two repo-wide
   discovery invariants H-I — no E (no rewash), no G (no sync_funds))
   wired into `.githooks/pre-push`.
5. Unit tests covering:
   - Helpers (month-end arithmetic, year wrap, leap-Feb, tz-naive
     `now` rejection, exact 24-month admission set).
   - Legacy ingester (`parse_submissions_index` intrinsic floor +
     caller-floor merge, `_ingest_single_accession` defensive gate
     for NULL `reportDate` leakage).
   - Manifest worker post-parse gate.
   - Bulk dataset per-row gate + counter.
   - Accessor cohort filter + bootstrap dispatch param resolution.
6. Spec §4.6 + §6.3 + §7 + §12 amend for PR7 SHIPPED state + PR8
   handover.

**Out**:

- N-CSR / N-CSRS (PR8 — already at `horizon_days=730`, spec §4.12
  says retain as-is; PR8 is doc-only unless drift found).
- `ownership_funds_current` size audit (PR12 — same audit as
  `ownership_institutions_current`).
- DELETE of pre-cap rows (spec §6.3 — caps are ingest-side only).
- Rewash `_apply_n_port_*` — does not exist; the rewash gate from PR5
  / PR6 has no N-PORT counterpart.
- `sync_funds` SQL predicate — `ownership_observations_sync.py` has
  no `sync_funds` function today. If it is added later, the lint
  guard will need an extra invariant analogous to PR6 §6 G; out of
  PR7 scope.

## 2. Settled decisions cross-reference

- **Spec §6.3 — no piecemeal deletes**: PR7 is ingest-side only. NO
  `DELETE FROM ownership_funds_observations` against pre-cap rows.
- **Spec §4.6 — depth + cohort**: PR7 lands BOTH (no prior cohort
  ticket; #1010 was 13F-only).
- **#1208 dev-DB hygiene**: 8q cap drops observations partitions
  outside the window from the clean re-run's footprint.
- **Settled — write-through ownership**: `ownership_funds_current`
  remains the latest-snapshot write-through table. PR7 cap shapes
  observations writes; current refresh fires only when an in-cap
  accession lands (consistent with PR4/5/6).

## 3. Review-prevention-log cross-reference

- **`date.today()` returns local TZ** (#1010 Codex 2) — helpers
  compute `datetime.now(tz=UTC).date()` for the cutoff anchor + reject
  tz-naive `now` arguments (mirror PR6 §5.1).
- **Bare-param counting let unused params or comment mentions inflate
  the count** (PR4 Codex 2 MED finding) — lint guard counts the
  FULL predicate expression (`period_of_report < retention_cutoff`),
  not the bare `%(cutoff)s` token.
- **BSD vs GNU `grep -P` portability** (PR4 Codex 1c lesson) — lint
  guard uses `awk` block parsing, no `grep -P`.
- **Universal-gate supersession** — PR7's intrinsic floor in
  `parse_submissions_index` adds the cap as the DEFAULT effective
  floor (caller `min_period_of_report=None` still gets the cap);
  a caller passing a tighter floor (bootstrap stage 22's 380d / 4q)
  takes effect via `max()`, preserving the explicit-opt-out shape
  (no silent neutering — Codex 1b PR6 lesson applied).
- **Cumulative ownership rollup** — N-PORT (unlike Form 4) writes
  point-in-time snapshots, not cumulative positions; `_current` is
  derived from `_observations`. Spec §4.6 + spec §6.3 hold: post-wipe
  + clean re-run rebuilds `_current` from 8q of `_observations` with
  no opening-balance loss (no cumulative state to preserve).

## 4. Two-axis analysis

### 4.1 Why period-based, not filed-at-based

N-PORT-P amendments (`NPORT-P/A`) can re-state filings for prior
fiscal quarters. A `filed_at`-only gate would let a 2026-05 amendment
of a 2022-Q3 filing slip through, polluting the "last 8 quarters"
semantics for that fund.

→ Gate on `period_of_report` (the month-end intrinsic to the
filing's content), not `filed_at`.

### 4.2 Month-end-anchored cutoff (not floating, not quarter-end)

N-PORT-P period_end is a **month-end**. Anchoring to calendar
quarter-ends (PR6's 13F shape) would systematically reject
fiscal-Q-non-calendar funds — e.g. a fund whose fiscal Q ends Jan 31
files period_ends of Jan 31 / Apr 30 / Jul 31 / Oct 31, none of which
land on Mar/Jun/Sep/Dec. A quarter-end-anchored cutoff would admit
zero observations for this fund.

A floating `today - 760d` cutoff has the same drift-to-9 problem PR6
caught for 13F (Codex 1a §1 BLOCKING on PR6 plan).

**Resolution**: anchor the cutoff to **calendar month-ends**. Compute
the cutoff as the month-end exactly `(NPORT_RETENTION_QUARTERS * 3) - 1`
= **23 months** before the most recent COMPLETED calendar month-end
(today's month minus one). Boundary inclusive → exactly 24 calendar
month-ends survive at every moment of every day. For any fund whose
fiscal Q ends on any month-end, exactly 8 of its fiscal-Q snapshots
fall within the admitted window (proof in §5.1).

→ Calendar-month-end cutoff with 24-month rolling window.
Implementation in §5.1.

### 4.3 Caller-driven vs intrinsic floor

Today: `parse_submissions_index(payload, min_period_of_report=None)` →
full history. The 13F PR6 pattern: caller-provided
`min_period_of_report` becomes the *additional floor*; the cap is
enforced as the *default*. Effective floor =
`max(caller_floor, n_port_retention_cutoff())`. A caller passing
`None` still gets the cap. A caller passing a tighter floor (e.g.
bootstrap stage 22's 380d) overrides upward.

### 4.4 Cohort bound — 380d recency, full daily

#1010 lesson: bootstrap stage filters cohort to recently-active
filers; daily standalone path uses full cohort (safety-net for
previously-inactive filers re-emerging). PR7 applies the same shape
to N-PORT:

- Bootstrap-stage 22: `min_last_seen_filed_at = today - 380d`
  (resolved at dispatch via `_PARAM_DYNAMIC_BOOTSTRAP_NPORT_CUTOFF`).
- Daily scheduler / Admin "Run now" / manual sweep: full cohort
  (kwarg defaults to `None`).

380d (not 730d) is the right window even though the depth cap is 2y:
inactive funds (no NPORT-P in 380d) are very unlikely to file the
next 8 quarters and bootstrap stage 22 wall-clock benefits more from
the tight cohort than from a wider safety margin.

## 5. Implementation

### 5.1 Constants + helpers (`app/services/n_port_ingest.py`)

Insert immediately after the existing `_NPORT_FORM_TYPES` constant
block (after line ~96):

```python
# ---------------------------------------------------------------------------
# N-PORT 8-quarter retention cap (#1233 PR7, spec §4.6)
# ---------------------------------------------------------------------------

# NPORT-P period_of_report is a calendar MONTH end (END of the third
# month of the fund's fiscal quarter; funds have their own fiscal
# calendars so the month can be any of Jan-Dec). Spec §4.6 caps depth
# at 8 fiscal-quarter snapshots per fund. Anchoring to month boundaries
# (not a floating ``today - 760d`` window, not calendar quarter-ends
# like 13F) admits 24 calendar month-ends = exactly 8 fiscal-Q
# snapshots per fund regardless of fiscal-year alignment.
#
# Ingest-side cap only — existing rows are untouched until the
# operator-driven pre-wipe + clean re-run (spec §6.3). Cutoff is
# computed in Python and passed as a ``date`` everywhere, NOT as
# ``NOW() - make_interval(...)`` which carries DB session-timezone
# ambiguity. UTC anchor — #1010 Codex 2 lesson: ``date.today()``
# returns local TZ, drifts the cutoff by ±1 day on non-UTC dev hosts.
NPORT_RETENTION_QUARTERS: int = 8


def _last_day_of_month(year: int, month: int) -> date:
    """Return the canonical month-end ``date`` for (year, month).

    Local helper — month-end arithmetic is a one-off here; importing
    dateutil for ``relativedelta`` would be casual dependency creep
    (CLAUDE.md non-negotiables).
    """
    if month == 12:
        return date(year, 12, 31)
    return date(year, month + 1, 1) - timedelta(days=1)


def n_port_retention_cutoff(now: datetime | None = None) -> date:
    """Earliest ``period_of_report`` accepted for NPORT-P / NPORT-P/A.

    Returns the calendar month-end exactly
    ``(NPORT_RETENTION_QUARTERS * 3) - 1`` = 23 months before the
    most recent COMPLETED calendar month-end (today's month minus
    one). Boundary inclusive → exactly 24 month-ends survive at every
    moment of every day, which is exactly 8 fiscal-Q snapshots per
    fund for any fiscal-year alignment.

    ``now`` must be a tz-aware datetime; the helper normalises to UTC
    before taking ``.date()`` so the cutoff doesn't drift on non-UTC
    callers (#1010 / PR6 Codex lesson).
    """
    if now is None:
        now = datetime.now(tz=UTC)
    if now.tzinfo is None:
        raise ValueError(
            "n_port_retention_cutoff: ``now`` must be a tz-aware datetime; "
            "naive datetimes would honour the caller's local TZ and drift the cutoff."
        )
    today = now.astimezone(UTC).date()
    # Walk back to the START of the month 24 months ago (= last day
    # of that month, inclusive boundary). ``today.month - 1 - 23``
    # = ``today.month - 24``; modular arithmetic on (year, month).
    months_back = NPORT_RETENTION_QUARTERS * 3  # 24
    target_y = today.year
    target_m = today.month - months_back
    while target_m <= 0:
        target_m += 12
        target_y -= 1
    return _last_day_of_month(target_y, target_m)


def n_port_within_retention(
    period_of_report: date | None,
    now: datetime | None = None,
) -> bool:
    """Boundary check used by every N-PORT writer chokepoint.

    Returns True iff
    ``period_of_report >= n_port_retention_cutoff(now)``.
    A None ``period_of_report`` returns False — defensive: an
    accession we couldn't tag with a month end is unsafe to admit.
    """
    if period_of_report is None:
        return False
    return period_of_report >= n_port_retention_cutoff(now)
```

Imports: `timedelta` is **not yet** in `n_port_ingest.py`'s import
block (`from datetime import UTC, date, datetime`). Add `timedelta`
to that import.

**Proof — 24 consecutive month-ends admits exactly 8 per fiscal-Q
congruence class**:

Let `latest_completed = (Y, M-1)` (today's month minus one) and
`cutoff = end_of_month(Y, M-24)` after modular adjustment. The
admitted set is the calendar month-ends `{(Y, M-24), (Y, M-23), ...,
(Y, M-1)}` — exactly 24 consecutive completed month-ends, inclusive
of both endpoints.

Any fund with fiscal-Q ending on month `Q` files period_ends in the
congruence class `{m : m ≡ Q (mod 3)}` (i.e. every third calendar
month). Across any 24 consecutive calendar months there are exactly
`24 / 3 = 8` month-ends in each such congruence class. So the
admitted set contains exactly 8 month-ends compatible with any
fiscal-Q choice — independent of which calendar month the fund's
fiscal Q happens to end in (Codex 1a WARN 2 tightening).

For a fund whose latest filed period_end is `latest_completed` (or
≤ 3 months stale, per the SEC NPORT-P filing cadence), those 8
admitted snapshots are exactly the 8 most-recent ones; the 9th most
recent sits 27 months back (one congruence-class step beyond the
window) and is correctly rejected. ∎

### 5.2 Chokepoint A — `parse_submissions_index` (intrinsic floor)

Change the function signature to add `min_period_of_report` (it
currently doesn't have one — N-PORT walks full history today):

```python
def parse_submissions_index(
    payload: str,
    *,
    min_period_of_report: date | None = None,
) -> list[AccessionRef]:
    """..."""
    # PR7 #1233 §4.6 — apply the intrinsic 24-month cap as the effective
    # floor; any caller-provided ``min_period_of_report`` can RAISE the
    # floor but never lower it.
    intrinsic_floor = n_port_retention_cutoff()
    if min_period_of_report is None:
        effective_floor = intrinsic_floor
    else:
        effective_floor = max(min_period_of_report, intrinsic_floor)
    ...
    for i, accession in enumerate(accessions):
        ...
        period = _safe_iso_date(...)
        if period is not None and period < effective_floor:
            continue
        ...
```

Add `min_period_of_report` as a kwarg to `ingest_all_fund_filers` +
plumb through to the per-CIK `parse_submissions_index` call site
(mirror PR6 §5.2 wire-through). Bootstrap stage 22 passes a cohort
floor only (`min_last_seen_filed_at`, §5.7); it does NOT pass a
period floor — N-PORT's monthly per-fund cadence makes the cohort
filter sufficient and adding a period sentinel is marginal-extra
without clear benefit (resolved Codex 1a WARN 1; previous draft of
this section + §11 contained contradictory language).

### 5.3 Chokepoint B — `_ingest_single_accession` defensive post-parse gate

If `parse_submissions_index` sees a NULL / malformed `reportDate`,
the accession leaks past the index-level gate (`period is not None`
short-circuits the comparison) and reaches `_ingest_single_accession`,
which then fetches `primary_doc.xml` + writes observations.

Defensive fix: after `parsed = parse_n_port_payload(primary_xml)`
succeeds in `_ingest_single_accession`, check the cap and short-circuit
to a `failed`-status outcome with `error='retention floor'`. Mirrors
the manifest-worker gate added in §5.4.

```python
# inside _ingest_single_accession, AFTER:
#     parsed = parse_n_port_payload(primary_xml)
# and BEFORE the first DB write (record_fund_observation /
# upsert_sec_fund_series).

# PR7 #1233 §4.6 — defensive post-parse gate. ``parse_submissions_index``
# already skips accessions with a known pre-cap ``period_of_report``,
# but submissions JSON may carry a NULL / malformed ``reportDate`` —
# those leak past the index-level gate and reach here. Re-check
# against the parsed period and short-circuit before the first write.
if not n_port_within_retention(parsed.period_end):
    return _AccessionOutcome(
        status="failed",
        holdings_inserted=0,
        holdings_skipped_no_cusip=0,
        holdings_skipped_non_equity=0,
        holdings_skipped_short=0,
        holdings_skipped_non_share_units=0,
        holdings_skipped_zero_shares=0,
        error="retention floor",
        series_id=None,
        period_of_report=parsed.period_end,
    )
```

### 5.4 Chokepoint C — manifest-worker `_parse_n_port` (post-parse gate)

In `app/services/manifest_parsers/sec_n_port.py`:

Insert the cap check immediately after `parse_n_port_payload(...)`
returns successfully (currently line ~182), BEFORE the first
`record_fund_observation` call. Skips the per-holding write loop +
upsert phase for pre-cap accessions, writes an ingest-log row with
`status='failed', error='retention floor'`, and returns a tombstone
outcome.

```python
# PR7 #1233 §4.6 — 8-quarter retention cap. ``parsed.period_end``
# is the month-end intrinsic to the filing; gate on it (not
# ``row.filed_at``) so NPORT-P/A late amendments restating pre-cap
# fiscal quarters are correctly rejected. Tombstone the manifest row
# so the operator's `sec_rebuild` is the recovery path if the cap
# widens later.
if not n_port_within_retention(parsed.period_end):
    logger.debug(
        "N-PORT manifest parser: accession=%s period=%s pre-8q retention cap; tombstoning",
        accession,
        parsed.period_end,
    )
    try:
        with conn.transaction():
            _record_ingest_attempt(
                conn,
                filer_cik=filer_cik,
                accession_number=accession,
                period_of_report=parsed.period_end,
                status="failed",
                error="retention floor",
            )
    except Exception as exc:  # noqa: BLE001
        logger.exception(
            "N-PORT manifest parser: ingest-log INSERT failed accession=%s",
            accession,
        )
        return _failed_outcome(f"log error: {exc}", raw_status="stored")
    return ParseOutcome(
        status="tombstoned",
        parser_version=_PARSER_VERSION_NPORT,
        raw_status="stored",
        error="retention floor",
    )
```

Mirror PR6 §5.4 placement: AFTER `parsed = parse_n_port_payload(...)`
succeeded, BEFORE the first per-holding write loop.

### 5.5 Chokepoint D — bulk dataset `ingest_nport_dataset_archive`

Add a `rows_skipped_retention` counter to `NportIngestResult` (or
equivalent — confirm the dataclass name in
`app/services/sec_nport_dataset_ingest.py`).

**Placement** (Codex 1a WARN 3 lesson): the per-row gate fires
**as early as possible** inside the row loop — after the
`period_end` parse, BEFORE any CUSIP / series / fund / sub / reg
lookup. Pre-cap rows must not pay for those lookups, and the
counter must reflect raw retention skip (not "post-CUSIP-resolve
pre-cap retention skip") so the operator's per-archive log
separates retention noise from CUSIP-resolution noise.

```python
@dataclass
class NportIngestResult:
    ...existing fields...
    rows_skipped_retention: int = 0  # PR7 #1233 §4.6
    ...

# inside ingest_nport_dataset_archive, BEFORE the per-row write loop:

# PR7 #1233 §4.6 — 8-quarter retention cap. Cutoff resolved once
# per archive to avoid date-rollover during a multi-million-row drain.
retention_cutoff = n_port_retention_cutoff()
...
for holding in _iter_tsv(zf, "..."):
    # PR7 #1233 §4.6 — retention gate FIRST (before CUSIP / series
    # / sub-reg-fund lookup) so pre-cap rows don't pay for the
    # lookup cost and the counter is unconfounded with downstream
    # filters. Codex 1a WARN 3.
    period_end = _parse_period_end(holding)
    if period_end is None:
        # malformed row — defer to existing malformed-row handling
        ...
        continue
    if period_end < retention_cutoff:
        result.rows_skipped_retention += 1
        continue
    # ...CUSIP resolve, series lookup, etc. follow here...
```

Wire `rows_skipped_retention` into the per-archive logging in
`sec_bulk_orchestrator_jobs.py` (around line 662) so the operator
sees the skip count. Distinguish all-retention-skipped (no error)
from all-CUSIP-unresolved (RuntimeError), mirroring PR6 §5.5's
split.

### 5.6 No rewash chokepoint

`rewash_filings.py` has no `_apply_n_port_*` function. Confirmed by
repo-wide grep (investigator audit, 2026-05-20). PR7 lint guard does
NOT include a PR6-equivalent invariant F for rewash. If a future PR
adds N-PORT rewash, that PR is responsible for adding the gate +
extending the lint guard.

### 5.7 Cohort bound (#1010 mirror)

#### 5.7.1 Accessor

Add to `app/services/sec_nport_filer_directory.py`:

```python
def list_nport_filer_ciks(
    conn: psycopg.Connection[Any],
    *,
    min_last_seen_filed_at: datetime | None = None,
) -> list[str]:
    """Return zero-padded CIKs from ``sec_nport_filer_directory``.

    ``min_last_seen_filed_at`` (PR7 #1233 §4.6, mirrors #1010 for 13F):
    when provided, restricts the cohort to trust CIKs whose last
    NPORT-P / NPORT-P/A was filed at or after that timestamp.
    Bootstrap stage 22 passes ``today - 380d`` to collapse the cohort
    to active-recent trusts; daily / standalone paths pass ``None``
    (full cohort — safety-net for previously-inactive trusts
    re-emerging).
    """
    with conn.cursor() as cur:
        if min_last_seen_filed_at is None:
            cur.execute(
                """
                SELECT cik
                FROM sec_nport_filer_directory
                ORDER BY last_seen_filed_at DESC NULLS LAST, cik
                """
            )
        else:
            cur.execute(
                """
                SELECT cik
                FROM sec_nport_filer_directory
                WHERE last_seen_filed_at IS NOT NULL
                  AND last_seen_filed_at >= %s
                ORDER BY last_seen_filed_at DESC NULLS LAST, cik
                """,
                (min_last_seen_filed_at,),
            )
        return [str(row[0]).zfill(10) for row in cur.fetchall()]
```

#### 5.7.2 Scheduler refactor + native `JobInvoker` registration

`sec_n_port_ingest` is currently registered via
`_adapt_zero_arg(sec_n_port_ingest)` at
`app/jobs/runtime.py:250`. That adapter **discards the params
dict** (`del params` in `_adapt_zero_arg`), so a bootstrap dispatch
with `params={"min_last_seen_filed_at": ...}` would silently
no-op the filter (Codex 1a BLOCKING 2).

Resolution: mirror the PR1c #1064 `sec_13f_quarterly_sweep`
migration. Change the body to consume `params` natively and drop
the adapter from `_INVOKERS`:

```python
# app/workers/scheduler.py
def sec_n_port_ingest(params: Mapping[str, Any]) -> None:
    """..."""
    from app.providers.implementations.sec_edgar import SecFilingsProvider
    from app.services.n_port_ingest import ingest_all_fund_filers
    from app.services.sec_nport_filer_directory import list_nport_filer_ciks

    # PR7 #1233 §4.6 — cohort recency filter for bootstrap dispatch
    # (stage 22 passes ``today - 380d``); daily standalone path passes
    # no params → ``min_last_seen_filed_at = None`` → full cohort.
    min_last_seen_filed_at = params.get("min_last_seen_filed_at")
    if min_last_seen_filed_at is not None and not isinstance(min_last_seen_filed_at, datetime):
        raise TypeError(
            "sec_n_port_ingest: ``min_last_seen_filed_at`` must be a datetime, "
            f"got {type(min_last_seen_filed_at).__name__}"
        )

    deadline_seconds = settings.sec_n_port_sweep_deadline_seconds

    with _tracked_job(JOB_SEC_N_PORT_INGEST) as tracker:
        with (
            SecFilingsProvider(user_agent=settings.sec_user_agent) as sec,
            psycopg.connect(settings.database_url) as conn,
        ):
            ciks = list_nport_filer_ciks(
                conn,
                min_last_seen_filed_at=min_last_seen_filed_at,
            )
            summaries = ingest_all_fund_filers(
                conn,
                sec,
                ciks=ciks,
                deadline_seconds=deadline_seconds,
                source_label="sec_n_port_ingest",
            )
        ...
```

```python
# app/jobs/runtime.py — drop the _adapt_zero_arg wrap so params
# survive to the body:
JOB_SEC_N_PORT_INGEST: sec_n_port_ingest,  # PR7 #1233 §4.6 — native JobInvoker
```

Daily / Admin "Run now" / `SCHEDULED_JOBS` dispatches with an empty
params dict → `min_last_seen_filed_at=None` → full cohort.
Bootstrap stage 22 dispatch passes `{"min_last_seen_filed_at":
<resolved_datetime>}` → filtered cohort.

#### 5.7.3 Bootstrap-orchestrator dispatch

In `app/services/bootstrap_orchestrator.py`:

1. Add the sentinel + 380d resolution:

```python
_PARAM_DYNAMIC_BOOTSTRAP_NPORT_CUTOFF = "<dynamic:bootstrap_nport_cutoff>"
```

2. Extend `_resolve_dynamic_params`:

```python
if resolved.get("min_last_seen_filed_at") == _PARAM_DYNAMIC_BOOTSTRAP_NPORT_CUTOFF:
    resolved["min_last_seen_filed_at"] = (
        datetime.now(tz=UTC).replace(hour=0, minute=0, second=0, microsecond=0)
        - timedelta(days=380)
    )
```

3. Replace stage 22 spec:

```python
_spec(
    "sec_n_port_ingest",
    22,
    "sec_rate",
    "sec_n_port_ingest",
    # ``min_last_seen_filed_at`` (#1010 mirror for N-PORT) bounds the
    # cohort to trusts whose most recent NPORT-P / NPORT-P/A filed_at
    # is within the 380-day window. Collapses ~5k registered trusts
    # to ~3-4k actively-filing trusts and drops bootstrap stage 22
    # wall-clock proportionally.
    params={
        "min_last_seen_filed_at": _PARAM_DYNAMIC_BOOTSTRAP_NPORT_CUTOFF,
    },
),
```

4. Extend `JOB_INTERNAL_KEYS` (or equivalent guard list) so the
   sentinel key passes the param-validation that bootstrap-dispatch
   applies.

### 5.8 Backfill / data migration

None. Caps are ingest-side per spec §6.3. The cohort table already
has `last_seen_filed_at` + index (sql/126); no migration needed.
Pre-cap rows in `ownership_funds_observations` persist until the
operator's pre-wipe + clean re-run.

## 6. Lint guard — `scripts/check_nport_retention.sh`

PR5-style block-level placement guard. Awk-based for BSD/GNU
portability. Eight invariants total (six placement A-F + two
repo-wide H-I — no E for rewash, no G for sync_funds; placement
letters follow PR6's mapping for grep-ability):

**A. Helpers defined exactly once, in the canonical module.**
File: `app/services/n_port_ingest.py`. Count rule:

- `def n_port_retention_cutoff(` == 1.
- `def n_port_within_retention(` == 1.

**B. `parse_submissions_index` intrinsic-floor parity.**
File: `app/services/n_port_ingest.py`. Inside
`def parse_submissions_index(`:

- `n_port_retention_cutoff(` ≥ 1 call (the effective-floor
  computation).
- Marker line `period < effective_floor` ≥ 1 (proves the helper output
  is wired into the per-accession filter).

**C. `_ingest_single_accession` post-parse defensive gate.**
File: `app/services/n_port_ingest.py`. Inside
`def _ingest_single_accession(`:

- `n_port_within_retention(parsed.period_end` line MUST exist
  AND its line number MUST be > the line containing `parse_n_port_payload(`
  AND < the line containing the first `record_fund_observation(`.

**D. Manifest-worker post-parse gate placement.**
File: `app/services/manifest_parsers/sec_n_port.py`. Inside
`def _parse_n_port(`:

- `n_port_within_retention(` line MUST exist
  AND its line number MUST be > the line containing `parse_n_port_payload(`
  AND < the line containing the first `record_fund_observation(`.

**F. Bulk dataset archive-level + per-row gate placement.**
File: `app/services/sec_nport_dataset_ingest.py`:

- `n_port_retention_cutoff(` exactly 1 call AND its line number MUST
  be < the line containing `for holding in _iter_tsv(` (or whatever
  the per-row iterator anchor turns out to be).
- `period_end < retention_cutoff` predicate MUST appear between the
  archive-level cutoff resolution and the `record_fund_observation(`
  call.
- `rows_skipped_retention` field on the result dataclass + ≥ 1
  increment.

**H. Repo-wide writer discovery.**

- Exactly 3 production *.py files under `app/` (excluding
  `ownership_observations.py` and tests) call
  `record_fund_observation(`. Mirrors PR6 invariant H.

**I. Repo-wide writer discovery — table-level.**

- Exactly 1 production *.py file under `app/` contains
  `INSERT INTO ownership_funds_observations (` (note trailing
  column-list paren). Mirrors PR6 invariant I.

(Letters E + G intentionally skipped to keep the PR6/PR7 invariant
letter-to-chokepoint mapping stable across both guards.)

### 6.1 Pre-push wiring

Add to `.githooks/pre-push` after the existing
`check_13f_hr_retention.sh` line:

```bash
bash scripts/check_nport_retention.sh
```

## 7. Tests

### 7.1 Helper tests (`tests/services/test_n_port_ingest.py`)

- `test_retention_cutoff_anchors_to_month_end`: pin three known
  dates → exact expected cutoff month-ends. Cover:
  - Mid-month (e.g. 2026-05-15 → cutoff = 2024-05-31).
  - First of month (e.g. 2026-06-01 → cutoff = 2024-06-30).
  - Last of month (e.g. 2026-05-31 → cutoff = 2024-05-31).
- `test_retention_cutoff_year_wrap`: today in January →
  ``today.month - 24`` rolls back two years, target month = January
  two years prior (2026-01-15 → cutoff = 2024-01-31; not "previous
  year minus 1" — earlier draft expressed this incorrectly).
- `test_retention_cutoff_february_target_month`: targets that land
  IN February exercise the leap arithmetic. With the §5.1 algorithm
  ``today.month - 24``, the target month equals ``today.month``
  shifted exactly 24 months back → February-target dates only arise
  when ``today.month == 2``. So:
  - 2026-02-15 → cutoff = 2024-02-29 (leap-Feb).
  - 2025-02-15 → cutoff = 2023-02-28 (non-leap).
  (Codex 1a BLOCKING 1 fix — earlier draft incorrectly expected
  Feb cutoffs from a March ``today``; with this algorithm, Mar 2024
  → cutoff 2022-03-31 and Mar 2025 → cutoff 2023-03-31.)
- `test_retention_cutoff_admits_exactly_24_month_ends`: iterate all
  month-ends from cutoff to ``latest_completed`` month-end (today's
  month minus one); assert count == 24 AND every one returns True
  from `n_port_within_retention`.
- `test_retention_cutoff_rejects_one_month_before`: month-end exactly
  one month before cutoff returns False.
- `test_retention_cutoff_rejects_naive_now`: passing
  `datetime(2026, 5, 20)` (tz-naive) raises `ValueError`.
- `test_retention_cutoff_normalises_non_utc`: passing
  `datetime(2026, 5, 20, 23, 0, tzinfo=ZoneInfo("America/New_York"))`
  yields the same cutoff as the UTC equivalent (`2026-05-21 03:00 UTC`).

### 7.2 Legacy ingester tests (`tests/services/test_n_port_ingest.py`)

- `test_parse_submissions_index_applies_intrinsic_cap`: payload with
  9 accessions across 27 months → returns only the 8 in-window.
- `test_parse_submissions_index_caller_floor_raises`: caller passes
  `min_period_of_report = today - 6mo` → intrinsic cap is overridden
  upward, only 2 in-window accessions returned.
- `test_parse_submissions_index_null_report_date_passes_index_gate`:
  one accession with `reportDate=None` → leaks past
  `parse_submissions_index` (existing behaviour for unknown periods).
- `test_ingest_single_accession_defensive_gate_for_null_report_date`:
  feed the null-reportDate accession through `_ingest_single_accession`
  with a stubbed parser that returns a pre-cap `period_end` → outcome
  is `status='failed', error='retention floor'` AND
  `record_fund_observation` is NOT called.

### 7.3 Manifest worker tests (`tests/test_manifest_parser_sec_n_port.py`)

- `test_manifest_post_parse_gate_pre_cap_tombstones`: pre-cap accession
  → tombstoned outcome, ingest-log row written with `status='failed'`,
  `record_fund_observation` NOT called.
- `test_manifest_post_parse_gate_in_cap_writes`: in-cap accession →
  normal flow, holdings written.

### 7.4 Bulk dataset tests (`tests/test_sec_nport_dataset_ingest.py`)

- `test_bulk_per_row_gate_skips_pre_cap`: TSV with 3 in-cap + 2
  pre-cap holdings → `result.rows_skipped_retention == 2`, only 3
  rows written.
- `test_bulk_boundary_equality_admitted`: holding with
  `period_end == retention_cutoff` → admitted (boundary inclusive).
- `test_bulk_no_writes_all_retention_skip`: TSV with all pre-cap
  rows → no error raised, `result.rows_skipped_retention` reflects
  total, distinguishable from all-CUSIP-unresolved case.

### 7.5 Cohort accessor tests
(`tests/test_sec_nport_filer_directory.py`)

- `test_list_nport_filer_ciks_full_cohort`: seed 3 trust rows →
  returns all 3 ordered by `last_seen_filed_at DESC NULLS LAST, cik`.
- `test_list_nport_filer_ciks_with_recency_filter`: seed 3 rows
  with `last_seen_filed_at` = today / today-200d / today-400d →
  `min_last_seen_filed_at = today-380d` returns 2 (first two).
- `test_list_nport_filer_ciks_excludes_nulls`: row with
  `last_seen_filed_at=NULL` → excluded when filter active, included
  when filter is None.

### 7.6 Bootstrap dispatch tests
(`tests/test_bootstrap_orchestrator.py`)

- `test_stage_22_resolves_nport_cutoff_sentinel`: dispatch stage 22
  via orchestrator harness → resolved param is a
  `datetime(tz=UTC, hour=0, minute=0, ...)` exactly 380 days before
  the harness's clock.
- `test_stage_22_sentinel_idempotent_on_re_dispatch`: re-resolving
  the resolved value is a no-op (already a datetime, not a sentinel).

### 7.7 Spec-amend test (no code path; doc-only sanity)

Optional. Spec amend is reviewed; no test.

## 8. Spec amend

Edit `docs/superpowers/specs/2026-05-19-data-retention-rubric.md`:

- §4.6: change "**Ingest depth cap**: **8 quarters** at the parser,
  same as 13F + always-current snapshot." to a richer block that
  EXPLICITLY notes:
  - The cap is **8 quarter-snapshots per fund**, not 8 calendar
    quarter-ends. N-PORT funds file on their own fiscal calendars,
    so the cutoff anchors to **calendar month-ends** (24-month
    rolling window) rather than the calendar-quarter anchor used
    by 13F-HR (§4.5). For any fund whose fiscal-Q ends on any
    calendar month, exactly 8 of its snapshots fall in any 24
    consecutive completed month-ends (§5.1 proof) — so the
    semantics match 13F's 8-quarter intent without rejecting
    fiscal-Q-non-calendar funds (Codex 1a LOW 1).
  - Chokepoint coverage (parse_submissions_index intrinsic floor,
    _ingest_single_accession defensive gate, manifest-worker
    post-parse gate, bulk dataset per-row gate).
  - Cohort bound: bootstrap stage 22 dispatches with
    `min_last_seen_filed_at = today - 380d` (#1010 mirror); daily
    standalone path keeps full cohort. No migration needed —
    `sec_nport_filer_directory.last_seen_filed_at` + index already
    exist (sql/126).
  - Lint guard `scripts/check_nport_retention.sh` with invariants
    A/B/C/D/F/H/I; no E (no rewash) and no G (no `sync_funds`).
  - Existing rows untouched until §6.3 pre-wipe.
- §6.3: append "(PR5 + PR6 + PR7 precedent)" to the
  rewash-happy-path-uncapped clarification if applicable. (PR7 does
  not change rewash semantics — N-PORT has no rewash function — so
  the clarification stays as PR5 + PR6.)
- §7: mark PR7 as SHIPPED with the merge SHA; describe scope.
- §12: replace the PR7 handover block with the PR8 handover (PR8 =
  N-CSR / N-CSRS validate existing 2y horizon, doc-only).

## 9. Rollout

1. Branch `feature/1233-pr7-nport-8q-cap` from `main`.
2. Implement §5 in this order: helpers → chokepoint A → chokepoint B
   → chokepoint C → chokepoint D → cohort bound (§5.7) → lint guard
   (§6) → spec amend (§8).
3. Tests as you go (§7).
4. Pre-flight self-review against engineering skills.
5. Local gates: `uv run ruff check . && uv run ruff format --check . && uv run pyright && uv run pytest`.
6. Codex 2 pre-push.
7. Push + PR + poll review + CI.

## 10. Risks

- **Month-end anchor edge cases**: leap-Feb, year-wrap. Tests
  cover; helper algorithm is purely arithmetic on (year, month).
- **NPORT-P/A amendments restating pre-cap periods**: gate on
  `period_of_report`, not `filed_at` (§4.1). Amendments inherit the
  same `period_of_report` as the original, so a 2026 amendment of a
  2022-Q3 filing is correctly rejected.
- **Funds with fiscal-Q non-aligned to calendar Q**: 24-month
  anchor admits 8 fiscal-Q snapshots regardless (§4.2 proof).
- **Cohort bound dropping previously-inactive trusts**: by design.
  Bootstrap-only filter; daily / standalone keeps full cohort
  (#1010 lesson — safety-net for re-emerging filers).
- **Lint guard placement strictness**: the guard enforces line-order
  invariants which can break on routine refactors (moving a helper
  call across the parser-call boundary). Mitigation: PR4/5/6
  precedent shows this catches real bugs (Codex-equivalent gate);
  the placement is part of the contract.
- **`ingest_all_fund_filers` signature change**: adding a kwarg.
  Compatible default (`None`) so no existing call sites break.

## 11. Open questions for Codex 1a (resolved)

- ~~Should bootstrap stage 22 also pass `min_period_of_report`?~~
  **Resolved NO** (Codex 1a WARN 1). N-PORT's quarterly per-fund
  cadence means the 380d cohort filter alone collapses bootstrap to
  actively-filing trusts; a period sentinel would add complexity
  without a clear benefit. §5.2 no longer mentions a stage-22 period
  floor.
- ~~Should `list_nport_filer_ciks` accept an UPPER bound on
  `last_seen_filed_at`?~~ Out of PR7 scope; reached via existing
  `POST /jobs/sec_rebuild/run` per-CIK override.
- ~~Is there an N-PORT analog of the 13F `rows_skipped_retention`
  surface in `sec_bulk_orchestrator_jobs.py`?~~ Confirm during
  implementation; structure already mirrors the 13F line 662 surface
  per the inventory.

## 12. Definition of done

- All §1 In-scope items implemented.
- All §7 tests passing.
- `uv run ruff check .`, `uv run ruff format --check .`,
  `uv run pyright`, `uv run pytest` all green.
- `scripts/check_nport_retention.sh` exits 0 against the branch
  HEAD; intentional violation in a scratch branch exits non-zero.
- Codex 1a on this plan → BLOCKINGs addressed.
- Codex 2 pre-push on the branch → BLOCKINGs addressed.
- PR description complete (what / why / test plan + spec
  cross-reference + the chokepoint inventory) and reviewer can read
  the PR without prior context.
- Spec §4.6 + §7 + §12 amended in the same PR.
