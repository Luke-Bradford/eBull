# #3386 — hunt feature store: spec

Build step 4 of `2026-09-25-pattern-hunt-programme.md`. Refs #2437, #3383, #3385. Security: none. A research
cache and a pure view change; no broker, auth or order path, no new table.

**Status: v3.** Built on the #3385 harness as merged (`2026-09-26-3385-hunt-harness.md` v5, "the harness spec",
cited H). Codex ckpt-1 on v1 returned 52 findings (`2026-09-26-3386-ckpt1-v1-findings.md`, verbatim); v2 fixes the
design-level ones inline and binds the implementation ones to a slice by number (the #3385 lesson: an unfiltered
whole-spec round does not converge). A scoped round on v2 checked the 52 and found 15 gaps (7 incomplete fixes, 8
new), fixed in v3; see "Revision notes". No hunt has a budget, so no trial can yet be evaluated; on the dev DB
`hunt_trials` holds 0 rows, so a new model id orphans nothing there (finding 52: that is the only database checked).

## Premise, measured (2026-09-26, dev DB, discovery through 2008-12-31, `survivorship_free`)
The harness already holds a columnar panel in memory (`hunt_panel.load_hunt_panel`, `array` columns keyed by
session ordinal). It loads it **from Postgres on every trial**:

| step | seconds | size |
| --- | --- | --- |
| `load_hunt_panel` | 159.4 | 4,565 series, 13,362,847 bars, 104,590 dividends, 11,876 sessions |
| `hunt_compute._form_cohorts`, constant signal | 201.5 | 4,786 formations, mean 2,306 eligible |
| same, a one-bar-return signal | 221.8 | |

Reproduce with `/tmp/h3386/time_panel.py` and `/tmp/h3386/time_form.py` (posted on the PR). `_form_cohorts` ran
eligibility, scoring, selection, cohort construction and the descriptive tilts; no book return was computed, so
neither run is a look. Books were not timed. On these two runs the load is 42–44% of load + formation time; one
corpus and two trivial signals, so this is the order of the saving, not a general speed-up (findings 49–50).
`terminating_in_window` = 0 in that load: no **admitted, loaded** discovery series terminates before 2009 (#3384
Finding 1). The A/B below therefore exercises no terminal series from the corpus; the pure tests do (finding 48).

## Source rule
No published rule governs a research cache or a view's field set; each choice below is a **(construction)**,
bound into `HUNT_HARNESS_MODEL_ID` because `hunt_store` joins `MODEL_CODE_MODULES`. The rules it must not break are
H's: look-ahead ("Signals": bars ≤ t only, cut inside `rebased_view`), fitting ("A fitted value is a literal
constant in the spec"), registration before any outcome, and the audited door (validation and holdout prices are
read only after an access row commits). numpy and stdlib versions are recorded on the outcome, not bound in
identity (H contract decision 75), and that holds for the store too (finding 13).

## Part A — the persisted discovery panel
New pure module `app/services/hunt_store.py` (numpy + stdlib; no DB, no reader, no `hunt_harness` import):

**Parts, not a panel (findings 5–6).** `load_hunt_panel` is split: `load_panel_parts(conn, universe, through)`
returns sessions, series and `load_counts`; regime labels are attached by `hunt_panel` from a live
`_regime_labels(conn, sessions)` call on **every** path. The store holds parts only. Regime labels are descriptive
(the per-regime readout, H "Inference") and come from the benchmark series, which the key does not cover; they are
computed live today, and stay so (finding 4: the recorded store digest does not identify them, as today nothing
does).

**Layout (CSR, `.npy`, little-endian, fixed dtypes).** Series sorted by `series_id`, each series' dividends in
ex-date order; the arrays preserve the loader's iteration order and a test asserts it (finding 21).
- `series.npy` int64 [n, 5]: `series_id`, bar start offset, dividend start offset, `terminal_ordinal` or −1,
  termination class code or −1. Offsets are start offsets; the end is the next row's start, or the column length
  for the last series (finding 15). Class codes index `manifest.termination_classes`, the sorted
  `TerminationClass` values.
- bar columns, one file each, all of the total bar count: `ordinal` int64 (a **session index** into `sessions`),
  ratio `open`/`high`/`low`/`close`/`volume`, `traded_open`, `traded_close` float64, `exclusion` uint8.
- `dividend_ordinal` int64 (session index), `dividend_amount` float64.
- `sessions.npy` int64: `date.toordinal()` of each session (a **calendar** ordinal; finding 16 — the two kinds are
  named apart in code and validated: sessions strictly increasing, every session index in range).
- NaN is stored as NaN.

**Manifest** (`manifest.json`, written last): `format` = `hunt-store-v1`, `key`, `through`, `load_counts`,
`termination_classes`, and each array file's sha256. `store_content_sha256` = sha256 of the manifest's canonical
JSON (sorted keys, compact separators, UTF-8; containers are objects and arrays, every leaf a string or an
integer, anything else refused), so `load_counts` is covered (finding 9; v2 round 15).

**Read (findings 14, 17–19).** Each file is read to bytes once, hashed, and decoded from those bytes with
`allow_pickle=False`; the dtype, rank and length of every array and the CSR invariants are then validated
(offsets monotone and in range, unique sorted ids, ordinals strictly increasing per series and in range, exclusion
and class codes in their domains). Any failure — missing or malformed manifest, unknown format, key mismatch,
missing hash entry, hash mismatch, truncated or structurally invalid array — is a **miss**. Read returns a whole
panel's parts or nothing.

**Key (findings 1–3, 12, 30).** Computed in `hunt_store` (hashed), with the same canonical JSON, from: `format`,
`through`, the **registered** `universe_identity.form()` and `HUNT_HARNESS_MODEL_ID`, both handed from
`hunt_harness.compute_trial` to `hunt_panel.compute_trial` as arguments (v2 round 11; `evaluate` checked that
identity equal to the live one before registering, H contract decision 73), and a **quarantine identity** that
`hunt_panel` reads.
- The quarantine identity (finding 2; v2 rounds 1, 10) is new: sha256 of canonical JSON of, for the admitted
  series at the rule version, every `research_price_quarantine_coverage` row as (`series_id`, `first_bar`,
  min(`last_bar`, `through`)) and every `research_bar_quarantine` row with `bar_date` ≤ `through` and either flag
  false, as (`series_id`, `bar_date`, `range_usable`, `return_usable`), ordered. Exact flags, not counts; no price
  column. Size, dev DB 2026-09-26: 936 such rows ≤ 2008-12-31 of 16,326 at the rule version (`select count(*),
  count(*) filter (where bar_date <= '2008-12-31') from research_bar_quarantine where rule_set_version = <v> and
  (not range_usable or not return_usable)`).
- Recomputed on every trial, so a hit also requires today's quarantine state (v2 round 9), and recomputed after
  the computation next to the harness's universe-identity recheck: a difference is `HuntPanelError` (no outcome,
  retry).
- ⚠ The key is **metadata**, so it detects a change of rule, code, population, quarantine flags, or a series'
  counts and date range, **not** a change of a value or an interior bar date that keeps every count and range.
  v1's "never a wrong read" was wrong (finding 1). See residuals.

**Build (findings 7, 22–24, 26, 51).** On a miss, under the programme lock (inside `evaluate`):
1. remove any `var/hunt_store/.tmp-*` left by a crashed build;
2. `load_panel_parts` live; write every array and then the manifest into `var/hunt_store/.tmp-<key>-<pid>/`;
   release the live parts;
3. read the temp directory back with the full read check; failure is an infrastructure error;
4. the universe identity (the harness's reader, passed in as a callable) and the quarantine identity again, after
   the read-back (v2 round 3); on any difference remove the temp directory and raise `HuntPanelError` before
   anything is published;
5. if `var/hunt_store/<key>/` exists (a corrupt entry that missed), remove it; `os.replace` the temp directory into
   place. No fsync: after a process or system crash the entry may be absent or torn, and a torn entry fails the
   read check and is a miss, so it is rebuilt (v2 round 4). An identity change after publication leaves an entry
   under the old key, which no later trial asks for (v2 round 3).
Other keys' directories are **kept** (finding 10: deleting them defeats reproduction). They change only when the
model id, identity or through date does; `scripts/` gains no cleanup, the operator removes old ones by hand.
The live parts are released before the read-back. Peak memory is not estimated here; slice 1 measures it (peak
RSS on a miss and a hit) in the A/B (v2 round 7).

**Every trial computes on a panel read from the store**, hit or miss: one code path (finding 47).

**Discovery only (finding 27; v2 rounds 5, 12).** `hunt_panel.compute_trial` uses the store exactly when
`split == "discovery"`, with `through` = `split_end` (the harness's fixed discovery end). Independently,
`save_parts` refuses parts with a session after `through`, and `load_parts` refuses a manifest whose `through` differs
from the requested one or whose last session is after it. Validation and holdout load live every time. A price file on disk is a read that needs no access row; those splits run a handful
of pinned specs, so the saving is not worth the door question.

**Outcome record.** `statistics["load"]` gains `store_key` and `store_content_sha256`, identical on a hit and a
miss, and both `null` on validation and holdout (v2 round 14). Whether it was a hit is logged, not stored. While the directory exists the digest identifies the exact bytes
computed on; after removal it identifies them without reproducing them.

**Gated reader (finding 28).** `app.services.hunt_store` joins `_RESEARCH_PRICE_READER_MODULES` in
`tests/test_sealed_outcome_scripts_are_gated.py`. The test then refuses it to any `scripts/` or `app/services/hunt*`
module except `hunt_harness`, `hunt_panel`, the grandfathered `_PRE_HUNT_RESEARCH_READERS` and the test's existing
gated scripts, exactly as for the other readers (v2 round 6). Direct
`numpy.load` of the files is outside that import test, as notebooks and shells already are (H "Signals" residual;
finding 29).

## Part B — what a signal can see
Hunt 1's families (programme doc) against the view:

| family | needs | in the view today |
| --- | --- | --- |
| 1 overnight vs intraday | open, close ≤ t | yes |
| 2 liquidity / volume shocks | volume, prices ≤ t | yes |
| 3 scheduled flows | the session's date; ex-dates ≤ t | **no** |

Two additions, both cut **inside** `rebased_view` (finding 32), which gains the sessions and each series'
dividends as arguments:
- `SignalView.dates`: the session dates for indices 0 … t, as a lazy read-only prefix of the panel's `sessions`
  (a new unscaled prefix class; `Prefix` scales numbers, finding 31).
- `SeriesView.dividend_ordinals` / `SeriesView.dividend_amounts`: the series' ex-date session indices ≤ t, from
  two parallel arrays `PanelSeries` builds once from its dividend mapping at construction (v2 round 13: never per
  formation), cut by `bisect` like the bars, from the dividends' own ordinals (a dividend on a session with no bar is kept, finding 34), and the amounts
  multiplied by the same rebase factor k as the prices, so an amount sits on the as-traded basis of t.

The dividends are the panel's, the same records the books credit (H contract decision on dividends; the loader's
coverage JOIN, finding 40). `research_price_daily` is keyed (`series_id`, `bar_date`), so one record per ex-date
reaches the loader (finding 39). A NaN amount is passed through; the harness filters only the final score, so a
signal must itself treat a non-finite input (finding 37). A signal that avoids such a name changes which names the
books hold, not the cell refusal rule (H: `bad_dividend` refuses a cell a held position crosses; finding 38).

**Point-in-time assumption, declared (findings 35–36).** An ex-date ≤ t and its amount are treated as known on t.
The archive carries neither a declaration nor a revision timestamp, so this is assumed, not verified. The fields are
ex-dates, not payment dates.

**Not offered in v1, with the reason:**
- **Future ex-dates.** Not knowable on t without a declaration date. A signal may forecast one from ex-date history
  ≤ t (a quarterly cadence); that forecast and its constants are the signal's.
- **Exchange-calendar features** (pre-holiday, sessions to month end). A causal version needs **scheduled**
  closures only (2001-09-11 was not known on 2001-09-10). `market_calendar` does not expose that split publicly,
  and editing it moves `market_calendar.RULE_SET_VERSION`, which `bar_capture_certificate`, `strategy_mt1_books`
  and three scripts key on. Calendar-day arithmetic on `SignalView.dates` is available. #3387 asks for more in its
  family spec if it needs it.
- ⚠ **For #3387 (finding 45):** a feature equal for every name on t cannot separate arm from control, which hold on
  the same sessions. Scoring nothing on other sessions idles **both** books, so it does not identify a calendar
  premium; a constant score ties every name into the arm. A calendar effect is testable here only as *which names*
  on those sessions.

## Part C — "normalisers, ranks fitted within window only" and "seeds"
- **Fitted values.** The store and the view hold no fitted quantity: raw bars, dividends and dates ≤ t, and a
  rebase factor from the as-traded close on t. Cross-sectional ranks are the signal's, over the names eligible on t.
  Anything fitted over a period is a literal constant in the spec, fitted on discovery only (H "Fitting"). That is
  a provenance rule enforced by review, not by code (finding 44; H "Signals": code review, not a sandbox).
- **Seeds.** The harness is deterministic and `tests/test_hunt_signal_imports.py` refuses `random` and
  `numpy.random` to signals. Randomness reachable another way is the same review residual. No seed is in the key.
- **Build identity** as the issue lists it: raw manifests, mappings, split membership and termination rules through
  `universe_identity`; quarantine through the new identity; adjustment and calendar rules through the model id's
  reader rule sets; split bounds through the model id. Costs apply after the panel and bind through the spec's
  `cost_model_id`.

## Delivery
1. **Slice 1 (corpus rung): Part A.** Binds findings 14–26, 30, 46–47, 51. Pure tests on a synthetic panel:
   round trip equal field by field, NaN-aware, including order, `array` types, empty dividends, a dividend on a
   no-bar session, a terminal series and the last series' offsets; each miss cause (key, missing file, flipped
   byte, truncated array, bad dtype, bad offset, pickled array); a miss writes, validates and publishes; a crashed
   temp directory is removed; a corrupt entry is replaced; an identity change during the build publishes nothing;
   validation and holdout never touch the store. **Full-population A/B**, live parts against stored parts for
   discovery, as separate assertions (finding 46): sessions equal; series-id sets equal; `load_counts` equal; per
   series, every column NaN-aware and dividends equal; metric = series with any difference (must be 0); plus
   hit-path load seconds.
2. **Slice 2 (behavioural rung): Part B.** Binds findings 31–34, 41–43. Tests: both new fields end at t, including
   negative indices, slices and an ex-date exactly on t; a dividend after t is invisible; the amount rebases with the
   prices and is unchanged, within the price fields' own tolerance, by a later split (existing view tests' shape);
   the look-ahead probe covers both fields; the import allowlist is unchanged.

**Acceptance for #3386:** both slices merged; the A/B reports 0 differing series and equal sessions, ids and
counts; no trial evaluated.

## Declared residuals
- **Value edits under unchanged metadata** (findings 1, 3, 8, 11; v2 round 2): a price, volume, dividend amount,
  split-factor value or interior bar date changed with every count and range kept is invisible to the key, so the store serves the build's values.
  Without the store the same edit changes results under an unchanged identity (H contract decision 73); with it,
  `store_content_sha256` at least records which bytes were used. Archive re-ingests are attended corpus events; the
  operator removes `var/hunt_store/` after one. An A→B→A change during a build is not detected.
- **Quarantine state is not in the trial identity** (v2 round 8), as in H today: the registered `TrialSpec` binds
  `universe_identity`, not the quarantine flags. The store key and the post-computation recheck bound it within one
  computation; across a re-flag between two trials of one hunt, the trials see different masking, as they would
  without the store.
- The store is a file any process can read. Discovery needs no door; outcomes still need registration, and a direct
  read is outside the import test like any shell (finding 29).
- Signals can reach the lazy prefixes' backing arrays by attribute; code review, as today (finding 43).
- Rebase factor overflow on extreme ratios is the prices' existing arithmetic, now applied to dividends too
  (finding 41).

## Revision notes
- **v1 → v2:** the 52 ckpt-1 findings: parts without regime labels, CSR and manifest contract, the read check, the
  build order and crash handling, kept stores, discovery-only, the gated reader, the view fields' cut and
  representation, the point-in-time assumption, wording on the premise and residuals.
- **v2 → v3** (scoped round, 15): exact quarantine flags ≤ `through` instead of counts; the identity recheck after the
  read-back and after the computation; the registered identity handed to the key; torn entries are misses; the
  store's own `through` checks; `null` store fields off discovery; the canonical JSON's leaves; dividend arrays
  built once; the memory estimate replaced by a measurement; interior dates and the unbound quarantine state as
  residuals.
