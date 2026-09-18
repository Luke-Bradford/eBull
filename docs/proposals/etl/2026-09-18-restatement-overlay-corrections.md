# #3189 findings 14 / 15 / 16 — corrections to the #2215 restatement overlay

**Status:** proposal · **Issue:** #3189 (findings 14, 15, 16) · **Builds on:** #2215
(`docs/proposals/etl/2026-09-18-blockholder-restatement-overlay.md`)

One 20-line helper — `app/services/ownership_rollup.py::_blockholder_restatement_holders`
(`:4017`) — carries all three. They are one slice because they share the helper, the
scan and the population. **Codex checkpoint 1 surfaced a fourth defect in the same
selection expression** (superseded amendments, F14b below), which is fixed here because
no correct version of that expression can leave it standing; **checkpoint 2 then
corrected the FIX twice** — supersession crosses the 13D/13G tags because upstream's
chain does, and the overlay emits one row per surviving holder, not per identity found.

## What the overlay is for

#2215: a >5% beneficial owner who ALSO files Form 3/4, DEF 14A or a 13F loses the
Rule 13d-3 cross-channel MAX and is classified into that other category, so the
`blockholders` wedge is omitted from `slices` entirely. An operator then cannot
distinguish *"no 13D/G filer holds >5%"* from *"one does, but their other channel won
the dedup"*. The overlay makes the absence legible. It is non-additive
(`denominator_basis="cross_channel_restatement"`) and every additive consumer —
residual, concentration, `SanityChecks`, `count_additive_institutional_holders`, the
CSV sum invariant — branches on the basis. ⚠ The **sunburst is the documented
exception**: it is a category ALLOW-LIST, not basis-driven (#2215). Nothing here adds a
category, so the exception is untouched.

## Source rule

| decision | governing rule | where verified |
| --- | --- | --- |
| A >5% beneficial owner files Schedule 13D/G at all | Rule 13d-1 (Exchange Act §13(d)/(g)); structured XML mandated **2024-12-18** by the Rule 13d-1/2 amendments | `.claude/skills/data-sources/sec-edgar.md` §2.4, §2.4.1 mandate table, §7.7 coverage cliff |
| One beneficial owner is ONE unit of account, counted once at total beneficial ownership, classified by most-specific ROLE — never by which form they filed. Overlapping restatements MAX; distinct holdings SUM | Rule 13d-3; Item 403 (17 CFR 229.403) proxy beneficial-ownership table | `docs/review-prevention-log.md` "One beneficial owner, counted once — MAX overlapping, SUM additive" (#1640/#1889); data-engineer invariant **I14** |
| A later Schedule 13D/A **supersedes** the earlier filing by the same person — it is not a second holding | Rule 13d-2 (amendment obligation) | `_dedup_within_source` (`:2093`) implements exactly this: `as_of_date desc (NULL last) → accession desc`, and ships the superseded originals as `dropped_sources` (`:2155`) |
| Every member of a Rule 13d-5(b)(1) group is deemed to own the whole group's securities, so N identities legitimately restate ONE block — counted once | Rule 13d-5(b)(1); Rule 16a-1(a)(2) for the Section-16 control group | data-engineer invariants **I17** / **I24**; `_reconcile_13d_groups`, `_reconcile_insider_control_groups` |
| A joint accession can name **up to 100 reporting persons** — "the filer of an accession" is a SET, not a value | EDGAR Schedule 13D/G XML tech spec rev 2.2 | `.claude/skills/data-sources/sec-edgar.md` §2.4 |
| Tie-break between two filings still equal after the above | **No published rule exists.** Fixed BY CONSTRUCTION, reusing this repo's own settled sequence rather than inventing one, and extended to be total | `_dedup_by_priority` (`:1998`) pins `priority_rank → as_of_date desc (NULL last) → accession desc → source_row_id desc`; `_PRIORITY_RANK` (`:874`) gives `13d` and `13g` the SAME rank 3, deliberately |

## Full-population verification

`scripts/census_3189_restatement_overlay_defects.py`, over the population the #2215
census used: every instrument with ≥1 `ownership_blockholders_current` row (an
instrument with no stored blockholder row has no 13D/G channel to fold, win or
misattribute). Figures land in the PR body.

⚠ Two stated limits on the census's F15 discriminator, neither of which the FIX depends
on — once identity is carried on `DroppedSource`, attribution is decided at production
time from the holder the entry came from, and the census only sizes the population:

- `ownership_blockholders_current` is a CURRENT table. An accession can persist for
  co-filer B after co-filer A's row advanced to a later filing, so A reads as "not a
  reporter on this accession" when it was one. `misattributed` is therefore an upper
  bound.
- membership proves the identity appears on the accession, not that the selected share
  count is that identity's figure.

## F14 — the overlay misses the case where 13D/G WON

`_reconcile_owner_once` (`:2388`) branches on `"13f" in present`, not on "13f won":

```python
elif "13f" in present:
    biggest_13f = max(by_source["13f"], key=lambda h: h.shares)
    category = "etfs" if (biggest_13f.filer_type or "").upper() == "ETF" else "institutions"
    if bene_max_source is None or src_total["13f"] >= src_total[bene_max_source]:
        figure_src = "13f"
    else:
        figure_src = bene_max_source      # ← 13d / 13g
```

So when the 13D/G subtotal beats the 13F, the surviving row's `winning_source` is
`13d`/`13g`, its `dropped_sources` carry the **13F**, and the helper's
`d.source in _BLOCKHOLDER_SOURCES` filter finds nothing. No overlay row is emitted and
the `blockholders` wedge is still absent — the #2215 ambiguity reached by the opposite
arithmetic.

**Fix.** The helper's question is *"is this owner's 13D/G channel rendered outside the
blockholders wedge?"*. Today it can only answer for a channel that LOST. Admit the
surviving row itself as a candidate when `winning_source ∈ {13d, 13g}`.

## F14b — the selection can publish a SUPERSEDED figure (found at checkpoint 1)

`best = max(dropped, key=lambda d: d.shares)` treats every dropped `13d`/`13g` entry as
an overlapping restatement to MAX over. They are not all that.
`_dedup_within_source` (`:2155`) ships the **superseded originals** of an amendment
chain as `dropped_sources`, still tagged `13d`/`13g`. A filer who reported 100 on a 13D
and then 60 on the 13D/A that replaced it has both in the list, and the shipped helper
publishes **100** — a figure Rule 13d-2 says no longer holds.

**Fix — order the candidates by AS-OF FIRST, then shares.** The two rules the module
already states, expressed as one total ordering:

1. **a later filing supersedes an earlier one** by the same person, whatever direction
   the revision went (Rule 13d-2) — so `as_of_date` leads;
2. **two filings of the same vintage MAX**, never sum: one block through two lenses
   (I14, #1640/#1889) — so `shares` comes second.

⚠⚠ **NOT partitioned by source tag, and Codex checkpoint 2 killed the version that
was.** `_dedup_within_source` receives the 13D and 13G rows in ONE pool (`:5597`) and
groups them on `_identity_key` + `ownership_nature`, **not** on source — so a filer who
moved from a 13D to a 13G has a single amendment chain upstream. Treating the tags as
independent chains resurrects the retired 13D's figure while the pie carries the 13G's.

## F15 — an overlay row can be attributed to an identity that did not file it

`DroppedSource` (`:136`) carries `source`, `accession_number`, `shares`, `as_of_date`,
`edgar_url` — **no owner identity**. `_collapse_insider_control_group`'s docstring
(`:3458`) already names the consequence: *"`DroppedSource` has no CIK/name field — same
limitation as #1645"*.

Four passes append a DIFFERENT identity's rows to a representative's `dropped_sources`,
all of them correct arithmetic under Rule 13d-5(b)(1) / 16a-1(a)(2):

| pass | site | what it appends |
| --- | --- | --- |
| same-accession collapse (#1764) | `_collapse_same_accession_channel` (`:3494`) | co-filers on one accession |
| Section-16 control group (#1652/#2230) | `_collapse_insider_control_group` (`:3465`) | non-rep members "from BOTH channels" |
| 13D/G group collapse (#1645) | `_collapse_blockholder_group` (`:2755`) | non-rep group members |
| institutional family (#1644/#1649) | `_reconcile_institutional_families` | non-rep family constituents |

The helper then emits `Holder(filer_cik=holder.filer_cik, filer_name=holder.filer_name,
…, winning_accession=best.accession_number)` — the rep's name over another person's
filing, with a clickable EDGAR link that does not name them.

**Fix — give `DroppedSource` the identity it is missing, and attribute the overlay row
to the ENTRY, not to the holder it hung on.** Two new required fields (`filer_cik`,
`filer_name`), set at every producer from the holder the entry came from; `pyright`
enforces the sweep because they are required, not defaulted.

⚠⚠ **Filtering foreign entries out — the first design — is WRONG and checkpoint 1
killed it.** Where a same-accession collapse consumed co-filer B's 13D into
representative A, and A wins on Form 4, B's 13D is the ONLY 13D/G evidence on the
instrument. Filtering it deletes the overlay entirely and re-opens #2215 for that case.
Emitting it under B's true name is both honest and the thing #2215 exists to show.

⚠⚠ **ONE ROW PER SURVIVING HOLDER, not one per identity found** — Codex checkpoint 2
killed the per-identity version. Every 13D/G filing hanging off one surviving holder
describes ONE block: that is *why* the collapses folded them onto that holder, each
member being deemed to own the whole group's securities. Per-identity emission renders a
two-member 10M group as two 10M rows and a 20M slice total against a pie carrying one
10M block. The cardinality stays the one #2215 shipped; all that changes is WHICH NAME
goes on the row. The result is then de-duplicated globally on `(identity, accession)` —
the same filing reached by two representatives is one filing.

⚠ **Known residual, stated rather than silently accepted** (checkpoint 1): where a
group's block is rendered in the `blockholders` wedge under member B while member A's
own 13G restating the same block lands in `institutions`, A still emits an overlay row.
A did file that 13G, so the row is true; what it cannot say is that B's wedge row is the
same block. Detecting that needs group membership the overlay does not carry.

⚠ The CSV `__dropped:` audit rows (`:5780`) get the same correction — they stamp
`holder.filer_cik` / `filer_name` on every dropped row today, so a foreign entry is
misattributed there too. Same field, same defect.

⚠ The existing de-dup keys discard the newly meaningful identity:
`_reconcile_owner_once`'s `seen = {(d.source, d.accession_number)}` (`:2412`) and
`_fold_overlap_into` / `_collapse_owner_lots`' `(source, accession, shares)` triples.
A foreign entry occupying `(source, accession)` would suppress the survivor's own entry.
All three keys gain the identity.

## F16 — the tie is broken by set-iteration order

`max(dropped, key=lambda d: d.shares)` (`:4067`) returns the FIRST maximal entry, and
the list order derives from `losing_sources = [s for s in present if s != figure_src]`
in `_reconcile_owner_once` (`:2408`) where `present = set(by_source)`. `str` hashing is
salted per process, so the accession/URL/as-of date published for an owner whose 13D and
13G report the same figure can differ between two renders of identical data.

**Fix.** No published rule chooses between a 13D and a 13G at equal shares —
`_PRIORITY_RANK` gives both rank 3 deliberately. Fixed BY CONSTRUCTION, reusing
`_dedup_by_priority`'s sequence and extending it to be TOTAL over the fields
`DroppedSource` actually has (it has no `source_row_id`, so that final pin is not
available and `source` replaces it):

```
as_of_date desc (NULL last) → shares desc → accession_number desc → source desc
```

The leading two keys are F14b's regimes; `accession_number` and `source` are the
determinism pins. `source` is needed because `13d` and `13g` tie on `_PRIORITY_RANK`,
and `accession_number` is `str(row["source_accession"] or "")` upstream, so empty
accessions occur and a pair can otherwise tie outright. `edgar_url` needs
no key — it is `edgar_archive_url(accession)`, a function of a key already present.

⚠ **`_argmax_source` (`:2200`) has the same defect one level up** and a final sort alone
cannot repair it: `max(sources, key=lambda s: (src_total[s], -_PRIORITY_RANK[s]))` runs
over a list built from a `set`, so which of `13d`/`13g` becomes `figure_src` — and hence
which entries exist to select from — is itself process-dependent. It takes the same
trailing `source` key.

## Scope

Read-path only: no parser version bump, no stored value change, no migration. **
Definition-of-Done clause 10 (backfill / `sec_rebuild`) is N/A** and is stated rather
than skipped. Clauses 8, 9, 11, 12 apply.

`_DroppedSourceModel` (`app/api/instruments.py:4737`) is NOT widened — the identity is
internal to reconciliation; what the API publishes changes only in that an overlay row
now names the correct filer.

## Tests

| assertion | why it is separable |
| --- | --- |
| a 13D that beat a 13F emits an overlay row (F14) | the pre-fix helper returns `[]` here |
| an ETF-typed and an insider-categorised F14 winner both emit | the branch that produces them differs |
| a superseded 13D original at a LARGER figure does not win (F14b) | a shares-only MAX passes every other test |
| two filings of the same VINTAGE still MAX | a supersession-only fix would break the overlapping regime |
| supersession crosses the 13D/13G tags, because upstream's chain does | tag-partitioned supersession passes every other supersession test |
| one surviving holder emits ONE row however many filings it carries | per-identity emission inflates a group's slice total N× |
| a foreign dropped entry emits a row under the TRUE filer, not the rep (F15) | attribution is invisible to a count-only assertion |
| that row is NOT dropped when it is the only 13D/G evidence (F15) | a filter that drops everything also passes the line above |
| one filing reached by two reps emits ONE row (global de-dup) | otherwise the slice total reads 2× |
| a foreign entry does not suppress the survivor's own entry in `_reconcile_owner_once`'s `seen` | the de-dup key widening has no other observable effect |
| two tied entries pick the same one under a permuted input order (F16) | the defect is order-sensitivity, so the test must permute |
| `_argmax_source` picks the same source under a permuted set (F16) | different function, same defect |
| NULL `as_of_date` and empty accession sort last, not first | NULL ordering is the classic inversion |
| the CSV `__dropped:` row names the true filer | different consumer, same field |
