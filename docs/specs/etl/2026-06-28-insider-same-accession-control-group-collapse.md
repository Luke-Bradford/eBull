# Insider/blockholder same-accession control-group collapse (#1764)

## Problem
`ownership_insiders_current` (and rarely `ownership_blockholders_current`) double-counts a single
beneficial-ownership position when a Form 4/3 (or 13D/G) control chain reports the SAME block more
than once on ONE accession: the controlling person (indirect) and the entity (direct), or a stack
of fund entities, each get a row with the SAME `shares` from the SAME `source_accession`. A
`SUM(shares)` rollup counts the one position N times.

Example — BKTI (instrument_id 13585), accession `0001104659-24-090956`:
- `Horowitz Joshua` — indirect 90,000 (controls the fund)
- `Palm Global Small Cap Master Fund LP` — direct 90,000 (the fund)

Both report the same 90,000 block under Rule 16a-1 deemed beneficial ownership. Raw insider
`SUM` = 815,453; endpoint shows 795,453; true ≈ 705,453 (the 90k counted once).

## Why the existing #1652 pass misses it
`_reconcile_insider_control_groups` (#1652) collapses control groups bucketed by EXACT `shares`
value **across the whole instrument (cross-accession)**, gated by `_INSIDER_GROUP_MIN_SHARES = 1M`
+ a non-round guard. That floor exists to suppress false positives in the FUZZY cross-accession
value-match (independent equal-grant directors coincidentally sharing a round number). It is the
right guard for that signal but it means every sub-1M same-accession dup leaks onto the endpoint.

## Source rule
SEC **Rule 16a-1(a)(2)** (and Rule 13d-3) — beneficial ownership is "deemed": a controlling person
and the entity it controls each report the SAME shares as their own beneficial position. The block
exists ONCE; the joint filing lists it under each deemed owner. Count it once (data-engineer I14:
"MAX overlapping, SUM additive"; prevention-log "deemed/overlapping = MAX, not additive"). A single
Form 4/3 accession listing the same `shares` under ≥2 distinct reporting persons is, by the form's
own structure, one deemed block reported jointly — structurally unambiguous, unlike the fuzzy
cross-accession value-match.

## Full-population verification (passes the #1659 trap)
Per prevention-log #1659 (an exact-same-value clustering heuristic was falsified by a full-pop scan —
independent equal-grant executives coincidentally share exact values), the signal was verified on the
FULL sub-1M population on dev (2026-06-28) BEFORE speccing:

```
signature = (instrument_id, source_accession, shares>0) with ≥2 distinct holder_identity_key
in ownership_insiders_current:
  raw (all magnitudes): 1,255 groups / 813 instruments / 74.5B shares overcounted
  sub-1M (what #1652's floor misses): 316 groups / 253 instruments / 387M shares overcounted
in ownership_blockholders_current: 1 group / ~154k shares.
```

Every sampled group is a genuine Rule 16a-1 deemed-ownership **joint filing** — a fund GP/LP/management
chain + the controlling person, all reporting the SAME block on ONE accession (Voss Capital, OrbiMed,
Ridgemont/REP, Blackstone/Legence, Framework Ventures). The #1659 false-positive class (independent
equal-grant executives) does NOT appear here, because those file **separate** accessions (each exec
files their own Form 4). **Same-accession cleanly separates deemed groups from coincidental equal
grants for the #1659 class → no magnitude floor needed.**

### Why same-accession needs no roundness/tolerance proxy (the #1645 guard is unnecessary here)
The #1645 blockholder pass had to *infer* group membership from circumstantial signals (same period_end,
near-equal non-round values, tiered distinct-CIK tolerance) because separate 13D/G accessions carry NO
direct membership evidence — so it needs the roundness/tolerance guards as proxies against coincidence.
A **joint Form 4/3 (or 13D/G) accession is itself the direct group-membership evidence**: under the
joint-filing rules (Rule 16a-3(j); Rule 13d-1(k)) parties co-file ONE accession precisely because they
ARE a group with a control/deemed relationship. Unrelated holders never share one accession. So the
same-accession + same-exact-shares signal already HAS the evidence #1645 lacked; the roundness/floor
proxies are redundant here and would wrongly skip round-value deemed blocks.

**Residual FP (Codex ckpt-1 #4), accepted + bounded.** It is not logically impossible for two affiliated
co-filers on one accession to each hold an *equal-but-separate* block (collapse would under-count by one
member's block). This is rare (the full sub-1M scan found zero), bounded (one member's block, never a
runaway), and **audit-surfaced**: every collapse emits an `insider_control_group_collapse` /
`blockholder_group_collapse` `corrections_applied[]` entry naming each folded member + shares, so an
operator can see exactly what was counted once. Erring toward collapse is also the correct bias for an
over-count fix: the rare under-count is strictly smaller than the systematic over-count it removes.

### Critical FP guard — exclude DEF 14A
DEF 14A is the inverse case: all named holders sit on ONE proxy accession, so same-accession does NOT
separate independent equal-grant executives there (the exact #1659 FPs — BXP Koop+LaBelle @1,875,000,
FOXA Ciongoli+Tomsic @2,750,000 — ARE same-accession). DEF 14A is already a non-additive memo overlay
(#1659/I21) and a def14a-source Holder CAN reach `survivors` (matched proxy rows, `ownership_rollup.py`
~1018-1047). So the new pass is restricted to insider sources `{form4, form3}` and the blockholder
channel `{13d, 13g}` ONLY — never def14a, never 13f.

## Fix — read-path collapse, no backfill
The rollup computes live from `_current`, so this is a pure read-path pass; no migration, no re-ingest.

Add `_reconcile_same_accession_groups(survivors, blockholders) -> (survivors, blockholders, corrections)`
in `ownership_rollup.py`, called immediately BEFORE `_reconcile_insider_control_groups` (#1652) at the
existing pipeline site (~line 3090, after `_reconcile_institutional_families`):

- **Insider side**: among `survivors` with `winning_source in {form4, form3}` and `shares > 0`, group by
  `(winning_accession, shares)`. A group with **≥2 distinct `_identity_key`** collapses via the existing
  `_collapse_insider_control_group(cluster)` (rep = deterministic tie-break already in that helper;
  losers → `dropped_sources` + one `insider_control_group_collapse` correction). Survivors not in a
  collapsing group (incl. all non-form4/form3 survivors) pass through unchanged.
- **Blockholder side**: among `blockholders` (all `13d/13g` by construction) with `shares > 0`, group by
  `(winning_accession, shares)`. A group with ≥2 distinct `_identity_key` collapses via the existing
  `_collapse_blockholder_group(cluster)` (rep = `cluster[0]` after a deterministic shares-desc +
  tie-break sort; `blockholder_group_collapse` correction). Rest pass through.

**No magnitude floor, no roundness guard** — the same-accession + same-shares + ≥2-distinct-holder
signature is unambiguous (justified above). The `_INSIDER_GROUP_MIN_SHARES` floor + `_is_group_block`
roundness guard stay ONLY on the fuzzy cross-accession #1652 / #1645 paths.

### Codex ckpt-2 findings (resolved before push)
- **#1 empty-accession guard.** The readers coerce a NULL `source_accession` to `""`, so the eligible
  predicate requires `winning_accession.strip()` — otherwise two unrelated equal-share holders with no
  accession would bucket as `("", shares)` and wrongly collapse. (Dev: 0 such buckets today; latent.)
- **#2 cross-channel consume.** A folded insider member often ALSO restates the same deemed block on a
  13D/G (dev: 226 such pairs). Collapsing only the insider rows would orphan the loser's blockholder
  row (no longer a survivor → `survivor_keys` can't exclude it → owner-once re-counts it, *relocating*
  the double-count to the blockholders wedge). So each insider group also pulls in every blockholder
  row whose `(_identity_key, shares)` matches a group member; they fold into the insider rep's
  `dropped_sources` and are removed from `blockholders` — exactly как #1652 consumes cross-channel.
  Live-verified on WAY: 2 insider reps carry the consumed 13G restatement (Derby LuxCo, CPP) instead
  of orphaning. A loser's 13D at a *different* value (a larger full-group block) is NOT consumed.
- **#3 (rebutted).** "A joint 13D/G accession could list separate member stakes, not one block."
  Rule 13d-5(b): each group member is deemed to own the WHOLE group stake, so members report the
  IDENTICAL aggregate — equal shares on one accession is the deemed-group signature (same as #1645/I17).
  Genuinely separate stakes have *different* shares and so never share a `(accession, shares)` bucket.

### Ordering rationale
Placed BEFORE #1652 so the precise same-accession collapse runs first; #1652's cross-accession fuzzy
pass then sees one representative row (not the N dup rows) at that value and is unaffected (it only
fires ≥1M anyway). Placed BEFORE #1645 so the blockholder same-accession collapse precedes the fuzzy
near-equal clustering. Distinctness keyed on holder identity (not row), so a single person's
direct+indirect on one accession (distinct count = 1) is NOT touched — it flows to owner-once's
existing additive SUM, unchanged. Same-shares-EXACT requirement means two genuinely different positions
on one accession (different share counts) are never merged.

The collapse **preserves `dropped_sources`** (Codex ckpt-1 #3): both reuse helpers
(`_collapse_insider_control_group`, `_collapse_blockholder_group`) append the folded members to the
representative's existing `dropped_sources` and return the rep with the SAME `_identity_key` it had,
so the downstream `survivor_keys` exclusion (#1645) and `_reconcile_owner_once` see the representative
correctly and the folded members' provenance is non-lossy.

## Acceptance
- Insider rollup counts a same-accession control-chain position once across the full sub-1M population
  (387M overcount eliminated); blockholder 154k group collapsed.
- BKTI insider total: 795,453 → 705,453 (Horowitz indirect 90k + Palm Global direct 90k counted once).
- AAPL: Berkshire/Buffett same-accession pair (if same-accession) counted once.
- `corrections_applied[]` lists each collapse (kind + folded members).
- Panel (`AAPL`, `GME`, `MSFT`, `JPM`, `HD` + BKTI) renders on `/instruments/{symbol}/ownership-rollup`.

## Tests
- Pure-logic table tests on `_reconcile_same_accession_groups`: collapses a 2-holder same-accession
  same-shares insider group; does NOT collapse (a) same holder direct+indirect (distinct=1),
  (b) different shares same accession, (c) def14a-source rows on one accession (#1659 FP guard),
  (d) round/sub-1M values (no floor → still collapses, proving floor-independence), (e) blockholder
  same-accession group. Asserts the surviving rep + `dropped_sources` + correction shape.
- Dev-verify on the endpoint for the panel + BKTI.
