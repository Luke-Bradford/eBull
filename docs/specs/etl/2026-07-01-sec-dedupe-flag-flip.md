# SEC dedupe flag flip — stop `daily_research_refresh` duplicating SEC fetches

Part of #649 (remaining live scope: "incremental adapters for daily refresh
family" — root cause 2, full-scan adapters).

**Scope narrowed after Codex ckpt-1 (2026-07-01).** Originally planned to flip
both `enable_filings_fetch_dedupe` and `enable_sec_fundamentals_dedupe`.
Codex flagged real correctness gaps in the filings-side "replacement is
equivalent" claim (7-day vs 30-day master-index lookback window, seed CIKs
explicitly skip the master-index write-through so brand-new instruments would
lose `filing_events` population, and the master-index writer's
`primary_document_url` is the generic `-index.htm` landing page vs the
submissions-based writer's specific document URL — a real operator-visible
regression risk with no verification plan to close it). Per the honest test
("if I don't do this, is production actually fine?" — yes, current behaviour
already works), **this PR flips `enable_sec_fundamentals_dedupe` only**. The
filings-side flip is left at its current default (`False`, unchanged) — see
"Not in this change" below.

## Problem (verified on dev DB, 2026-07-01)

`daily_research_refresh` (scheduler.py:2709) does an unconditional full-scan
SEC fundamentals fetch + SEC filings fetch for all ~12k tradable instruments,
every day, taking 40-45 min. `job_runs` shows repeated failures:

```text
2026-06-26, 06-28, 06-29, 06-30: status=failure,
error_category=internal_error,
error_msg="orphaned: reaped at boot (owning worker thread died without a terminal status)"
```text

The 40-45 min runtime is a wide target for interruption by any dev-server
`--reload` restart, each one wasting the full run.

Two feature flags already exist to eliminate the duplicate work
(`app/config.py:227,240`), shipped in `bc5fb3ee` (#312, Chunk L) and
`d0307b84` (#414), both `default=False` today, and never flipped:

- `enable_filings_fetch_dedupe` — when True, `daily_research_refresh` skips
  its SEC filings block (scheduler.py:2807).
- `enable_sec_fundamentals_dedupe` — when True, `daily_research_refresh`
  skips its SEC fundamentals block (scheduler.py:2776) AND `fundamentals_sync`
  phase 1b (scheduler.py:3993) starts running instead.

Both ship-comments say the intended lifecycle is: "Ship as False (default) →
operator flips True → observe → follow-up PR deletes the guarded block."
`docs/settled-decisions.md:360` explicitly names "Chunk L flag-flip... may
proceed once its own issue is prioritised" — pre-approved, not a reversal.

## Verified coverage (falsifying "is the replacement actually equivalent")

**Fundamentals side** — `enable_sec_fundamentals_dedupe`: this flag is
symmetric (both `daily_research_refresh`'s block and `fundamentals_sync`
phase 1b gate on the SAME flag, mutually exclusive — NOT already-duplicated
today). Flipping it MOVES the `refresh_fundamentals` call (same function,
same cohort) out of the giant 40-min job into the isolated ~3-min
`fundamentals_sync` job (which already runs daily, consistently succeeds,
right after the 02:30 UTC SEC XBRL publish).

**Full-population cohort-equivalence check (dev DB, 2026-07-01)** — the two
call sites use textually different SQL and, critically, resolve their cohort
at different grains:

- `daily_research_refresh` selects all tradable instruments, then keeps those
  whose `UPPER(symbol)` is a key in a symbol→CIK dict built from primary-CIK
  tradables (`scheduler.py:2727,2776`). Membership is decided at **symbol
  grain** — if two tradable instruments shared a symbol and only one carried a
  primary CIK, BOTH would pass the `sym.upper() in cik_map` filter.
- `fundamentals_sync` phase 1b joins each instrument directly to its own
  primary-CIK row (`scheduler.py:3999`). Membership is decided at
  **instrument_id grain** — an instrument is included only if it has its own
  primary CIK.

These grains diverge iff a tradable symbol is non-unique. An earlier draft of
this proof diffed `(symbol, cik)` SETS, which would have MASKED exactly that
divergence (both paths yield the same symbol regardless of instrument_id
count). Re-ran the diff at the correct **instrument_id grain** over the full
table (`scripts`-style ad-hoc, not a sample), and separately counted duplicate
tradable symbols:

```text
PathA (daily_research_refresh) instrument_id set size: 5343
PathB (fundamentals_sync phase 1b) instrument_id set size: 5343
A - B (in daily, not phase1b): 0
B - A (in phase1b, not daily): 0
duplicate UPPER(symbol) groups among tradables: 0
```text

Zero divergence at instrument_id grain, and zero duplicate tradable symbols —
so the two grains coincide today and the equivalence is exact, not
accidental. Same `refresh_fundamentals` call, same cohort, different (faster,
isolated, already-proven-reliable) wrapper job — no coverage change. (If a
future duplicate-symbol tradable is ever added, PathA would over-include the
CIK-less twin; the post-merge verification below re-runs this instrument-grain
diff so that regression cannot land silently.)

## Change

Flip `enable_sec_fundamentals_dedupe` default in `app/config.py` False → True.
`enable_filings_fetch_dedupe` stays `False` (unchanged — see below). No schema
change. No new tests needed — `tests/test_daily_research_refresh_dedupe.py`
and `tests/test_fundamentals_sync.py` already parametrize both flag states
explicitly via `monkeypatch`, independent of the `Settings` default.

## Not in this change (deferred, correctly — see #649)

- **`enable_filings_fetch_dedupe`** — Codex ckpt-1 found the "replacement is
  equivalent" premise does NOT hold: `daily_financial_facts` uses a 7-day
  master-index lookback vs `daily_research_refresh`'s 30-day window; seed
  CIKs (brand-new instruments) explicitly skip the master-index
  `filing_events` write-through (`fundamentals/__init__.py:2878`), so newly
  added tradable instruments would lose filings population entirely if the
  guarded block were disabled; and the master-index writer's
  `primary_document_url`/`source_url` is the generic `{accession}-index.htm`
  landing page vs the submissions-based writer's actual specific document
  URL — a real operator-visible link-quality regression with no fix designed.
  Needs its own dedicated investigation (close the 30-day lookback gap or add
  a seed-CIK rescue path, and decide whether URL quality matters enough to
  backfill) before it's safe to flip. Left at current default; not a "no
  punting" violation — the honest test says production is fine without this
  change today, and the premise this optimization rests on is falsified as
  currently designed.
- Deleting the fundamentals-side guarded block entirely (the "follow-up PR"
  the original comments promised) — deferred one more cycle so the flip gets
  a real production observation window first, per the documented lifecycle.
- Companies House filings refresh — no incremental/dedupe sibling exists;
  genuinely separate new-feature scope, not a mechanical flag flip.

## Verification plan (post-merge, same session)

1. Restart jobs daemon onto new main (owned per standing instructions —
   `app/workers/scheduler.py` changed).
2. Manually trigger `fundamentals_sync` and `daily_research_refresh` via the
   admin job-run endpoints and confirm via `job_runs`:
   - `daily_research_refresh` log shows the SEC-fundamentals block skipped.
   - `fundamentals_sync` log shows phase 1b actually executing (`attempted > 0`
     — NOT `upserted > 0`, since 0 upserts is legitimate when rows are already
     current-quarter fresh).
3. **Full-population freshness safety signal (not a sample).** Count
   CIK-primary tradable instruments whose `fundamentals_snapshot` is missing or
   stale (as-of > 1 trading day) before and after the flip; the post-flip
   count must not exceed the pre-flip count. A 5-ticker panel (AAPL, GME, MSFT,
   JPM, HD) is a readability spot-check ONLY, not the safety signal — the
   full-pop stale count is what gates "no coverage regression."
4. **Re-run the instrument-grain cohort diff** (the check under "Verified
   coverage") post-flip and confirm `A - B == 0`, `B - A == 0`, and duplicate
   tradable symbols `== 0`. Guards against a duplicate-symbol tradable having
   been added between spec time and merge, which would let PathA over-include a
   CIK-less twin the flip then drops.

## Pause-semantics change (this flip is NOT pause-neutral — corrected 2026-07-02)

An earlier draft claimed both paths sit behind the same
`layer_enabled[fundamentals_ingest]` gate and that the flip is pause-neutral.
**That was wrong.** Verified against the code:

- `fundamentals_sync` checks `is_layer_enabled(fundamentals_ingest)` up front
  and returns before phase 1b when paused (`scheduler.py:3917`).
- `daily_research_refresh` has **no such gate** — its docstring says "No tier
  gate" (`scheduler.py:2718`) and nothing between entry and the SEC
  fundamentals block reads the pause layer.

So the flip DOES change pause behaviour, in the correct direction:

- **Today (flag=False):** pausing `fundamentals_ingest` stops `fundamentals_sync`
  phase 1b but `daily_research_refresh` keeps refreshing `fundamentals_snapshot`
  regardless — the operator pause is leaky (does not actually stop SEC
  fundamentals refresh).
- **After flip (flag=True):** `daily_research_refresh` skips its SEC block
  unconditionally and the only remaining SEC-fundamentals path
  (`fundamentals_sync` phase 1b) honours the pause — so pausing
  `fundamentals_ingest` now fully stops SEC fundamentals refresh, matching
  operator intent.

This is a strict improvement (a pause that pauses), not a regression, so it
does not block the flip — but it is a real observable behaviour change and is
recorded here rather than hidden. No code change is made to close the leak on
the flag=False path, because that path is being retired by this very flip.
