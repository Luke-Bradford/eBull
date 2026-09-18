# #2215 — make the folded 13D/G channel visible instead of absent

Status: proposal. Ticket: #2215. Predecessors: #1640 (owner-once), #919 (funds
overlay), #1659 (DEF 14A overlay), #1645 (13d-group collapse).

## The defect, restated precisely

`ownership_blockholders_current` holds 24,083 rows across **4,961** instruments.
On the golden panel every instrument has rows and none renders a blockholders
wedge:

```
AAPL  slices: insiders, institutions, def14a_unmatched, funds   (no blockholders)
GME   slices: insiders, institutions, def14a_unmatched, funds   (no blockholders)
MSFT  slices: insiders, institutions, def14a_unmatched, funds   (no blockholders)
```

The mechanism is `_reconcile_owner_once` (`ownership_rollup.py:2299`): an owner
lands in `blockholders` **only if** they filed no Form 3/4, no DEF 14A and no
13F. Vanguard / BlackRock / State Street file both a 13G and a 13F, so on any
large cap the 13G loses the cross-channel MAX and the owner is classified
`institutions`. The 13D/G figure becomes a `DroppedSource` entry on the
surviving holder.

**The arithmetic is correct and is not in scope.** `_PRIORITY_RANK` stays as it
is; prevention-log entry #1889 records the Cohen-on-GME double count (48.37%
"known" against a truthful 40.16%) that owner-once fixed.

What is wrong is that the card renders *"a 13D/G filer holds >5% but their 13F
won the dedup"* and *"no 13D/G filer holds >5%"* with identical pixels — the
category is omitted from `slices` entirely in both cases. Activist positioning
(13D) is decision-relevant and the wedge appears to promise it.

## Source rule

There is no SEC reg governing this: it is a presentation decision about data we
have already reconciled. The governing in-repo rule is
`docs/proposals/etl/ownership-full-decomposition.md` — the ticket names it and
it is the right document, with one correction recorded below:

> Each category declares its `denominator_basis` so totals never mix
> incompatible share bases. […] **`borrow_artifact`** — short interest.
> Sold-but-borrowed shares are still owned by the lender; counting them as a
> category would double-count. **Memo overlay only.**

That is the principle this change applies: a figure that is real, sourced, and
**already counted in another slice** renders as a non-additive memo overlay, not
as a wedge. `funds` (`institution_subset`, #919) and `def14a_unmatched`
(`proxy_disclosure`, #1659) are the two shipped instances.

⚠⚠ **The same document's dedup section is STALE on this exact point and must not
be followed.** Lines 177-186 specify that `(beneficial, 13d, 75M)` and
`(direct, form4, 38M)` for one CIK **both render**, on a two-axis
source × `ownership_nature` model:

> Insiders + blockholders: a CIK may produce one `direct` row (insiders slice)
> AND one `beneficial` row (blockholders slice).

`_reconcile_owner_once` (#1640) **reverses** that, on measured evidence — the
two-axis model is precisely what produced the GME double-count. The ticket's
instruction to "confirm against `ownership-full-decomposition.md`" therefore
points at a section that a later, evidenced decision superseded. Its
*memo-overlay* principle stands; its *dedup* model does not. Recorded here
because the next reader of that doc will hit the same trap.

## Full-population verification

`scripts/census_2215_blockholder_visibility.py` — read-only, calls
`get_ownership_rollup` per instrument rather than restating the dedup in SQL
(the fold is Python, downstream of six reconciliation passes; a SQL
identity-overlap query would measure a population the module does not have —
the failure #3104 slice 9 recorded).

It splits the population five ways, because the ticket's framing has two states
and the data has more:

| state | meaning | what a fix may say |
| --- | --- | --- |
| `rendered_complete` | wedge renders, nothing folded | honest today |
| `rendered_partial` | wedge renders AND another owner's 13D/G was folded | **wedge is UNDERSTATED** — not in the ticket |
| `absent_folded` | no wedge, ≥1 folded 13D/G channel | the ticket's case |
| `absent_upstream` | no wedge, NO folded channel anywhere | a "deduped into institutions" note here would be a **lie** |
| `no_rollup` | rollup could not be built | neither |

⚠ `absent_upstream` is why option 1 ("render an explicit *N blockholders deduped
into institutions* note") is rejected as specified: a blanket note would assert a
fold that did not happen on that instrument. The overlay is emitted **only when
there is a folded channel**, so it is silent in exactly the case where it would
be untrue.

**Full population, 4,961 instruments, 0 failures:**

```
rendered_complete   798   16.09%
rendered_partial   1495   30.14%
absent_folded      1555   31.34%
absent_upstream    1113   22.43%
no_rollup             0    0.00%

owners whose 13D/G channel is folded out of the wedge : 7633
  ... non-zero, and therefore rendered by the overlay : 6917
instruments that gain a blockholders_restated overlay : 2955
instruments rendering NO slices at all (no denominator):  921
```

**3,050 of 4,961 (61.5%) render a misleading blockholders state today** —
`absent_folded` plus `rendered_partial`. The second of those is not in the
ticket and is nearly as large as the one that is.

⚠ **The golden panel is not representative and must not be generalised from.**
All five panel names are `absent_folded`, but that state is only 31.3% of the
population; the panel is large-cap-biased, which is exactly the cohort where a
13F beats a 13G.

⚠ **2,955, not 3,050, is the number of instruments the fix changes.** 95 have a
folded 13D/G channel whose value is `0.0000`, which `_build_slice` drops (#1916
Finding A) — GME is one: a 0-share Vanguard 13G behind a real Cohen 13D, so its
2 folded owners render as 1 overlay row. The census reports both figures because
they answer different questions; neither is a substitute for the other.

⚠ **`absent_upstream` is not one thing.** 921 of its 1,113 instruments render
**no slices at all** (no usable `shares_outstanding`), so their empty
blockholders wedge is not a dedup artefact and no note of any wording would be
true of them. The remaining ~192 have a working rollup whose 13D/G rows were
removed before reconciliation.

## The change

**Option 2 from the ticket.** One new memo-overlay slice.

* Category `blockholders_restated`, `denominator_basis="cross_channel_restatement"`.
* Holders: one per owner whose surviving pie-wedge row carries a `DroppedSource`
  with `source ∈ {13d, 13g}`, at that owner's dropped 13D/G channel subtotal,
  carrying the dropped accession, as-of date and EDGAR url.
* ⚠ Built from **non-`blockholders` pie slices only**. An owner *inside* the
  blockholders wedge can also carry a dropped `13d`/`13g` entry — that is a
  within-channel supersession (`_dedup_by_priority` collapsing a 13G behind a
  13D), already represented by the surviving blockholder row. Including it would
  show one stake twice in the same panel.
* ⚠ Where one owner has both a dropped `13d` and a dropped `13g`, take the
  **max**, not the sum: they are the same stake restated (prevention-log #1889),
  so summing doubles the magnitude of a single invisible position.
* The slice is emitted only when it has ≥1 holder.
* ⚠⚠ Every field of an overlay holder comes from the **dropped entry**, not from
  the surviving holder it was attached to: `winning_source ∈ {13d, 13g}`,
  `winning_accession` / `winning_edgar_url` / `as_of_date` from the
  `DroppedSource`. Carrying the survivor's source would label a 13G figure with
  a 13F accession, and carrying the survivor's `as_of_date` would corrupt the
  slice's as-of coherence envelope (`_build_slice` derives it from holder
  as-of dates), which is a stated honesty contract of #1647 part 1.
  `dropped_sources` on an overlay holder is empty — the entry *is* the row.

### Scope: pie-wedge slices only, and that is complete rather than convenient

`_reconcile_owner_once` emits exactly four categories — `insiders`,
`blockholders`, `institutions`, `etfs` — all `pie_wedge`. `funds` and
`def14a_unmatched` are assembled on separate paths and never pass through it, so
a dropped `13d`/`13g` entry **cannot** exist on a memo-overlay holder. Scanning
pie slices is therefore exhaustive, not a sampling convenience, and it matches
the scope `build_rollup_csv`'s existing `__dropped:` loop already uses.

### ⚠ The CSV will carry the same fact at two granularities

`build_rollup_csv` already emits `__dropped:13d__` / `__dropped:13g__` rows —
one **per dropped filing entry**, at that channel's owner subtotal, stamped with
the rep filing. The new overlay adds one row **per owner**, at
`max(13d, 13g)`, under `__memo:blockholders_restated__`.

For an owner with only one dropped blockholder channel these are the same
number under two prefixes; for an owner with both, `__dropped:` emits two rows
and the overlay one. **Neither enters the documented sum invariant** (both
prefixes are excluded by construction), so no arithmetic breaks — but an
operator summing *memo* rows across prefixes would double-count. Documented in
the CSV docstring rather than suppressed: the `__dropped:` rows carry the full
per-filing audit trail (#1640's purpose) and the overlay row carries the
per-owner figure the card renders; deleting either loses information the other
does not have.

### Why the blast radius is small

Every downstream consumer of the rollup already branches on `denominator_basis`,
not on a category list — that genericity was built by #919 and re-asserted by
Codex at #1659's checkpoint 1. Verified in code, not taken from docstrings:

| consumer | behaviour with a new non-`pie_wedge` slice | evidence |
| --- | --- | --- |
| residual / concentration / `SanityChecks` | excluded from the math | `ownership_rollup.py:428` |
| L1 panel table | routed to `OverlaySection` automatically | `OwnershipPanel.tsx:558-561` (`_denominatorBasis(s) !== "pie_wedge"`) |
| CSV export | emitted under the `__memo:<category>__` prefix, outside the documented sum invariant | `build_rollup_csv`, `pie_slices` / `memo_slices` split |
| CSV `?category=` filter | identity map, so the new category scopes correctly | `rollup_csv_slice_filter` |

⚠ **One consumer is NOT basis-driven and the difference matters.**
`rollupToSunburstInputs` (`OwnershipPanel.tsx:199-220`) builds the chart from an
explicit **category allow-list** (`flattenHolders("institutions", …)` etc.), so
the new overlay is excluded because it is *absent from a list*, not because a
basis check rejects it. Same outcome, different mechanism — and a future
category added to that list leaks into the pie with nothing to stop it. Pinned
by `OwnershipPanel.test.ts` ("never flattens restated blockholders into the
chart"), revert-probed: adding one `flattenHolders("blockholders_restated", …)`
line fails that test and only that test.

What is **not** basis-driven and must be edited by hand:

1. `SliceCategory` (`ownership_rollup.py:69`) and the API `Literal`
   (`instruments.py:4860`) + `DenominatorBasis`.
2. `_ROLLUP_CSV_SLICE_CATEGORIES` (`instruments.py:5676`) — otherwise
   `?category=blockholders_restated` 400s while the rows are in the default export.
3. FE `OwnershipSliceCategory` union (`api/ownership.ts:40`) and
   `OwnershipDenominatorBasis` (`:64`).
4. `SLICE_TO_TABLE_CATEGORY` (`OwnershipPage.tsx:121`) — typed
   `Record<OwnershipSliceCategory, …>`, so this is a **typecheck failure** until
   mapped, which is the guard that stops a category silently vanishing from L2
   (prevention-log #1845).
5. `OverlaySection` copy (`OwnershipPanel.tsx`) — one branch, as `isFunds` already does.

### L2 filer table: mapped to `null`, deliberately, and said out loud

The two shipped overlays differ: `funds` is excluded from the L2 filer table
entirely, `def14a_unmatched` keeps its own table category "so the proxy holders
stay inspectable / exportable as a cross-check" (`rollup_csv_slice_filter`
docstring). #2215's purpose is inspectability, which argues for the DEF 14A
treatment.

This change takes the `funds` treatment (`null`) for one reason: the L1 overlay
already names the top holders and the CSV already carries every one of them, so
the operator can see *who*. Adding an L2 filer category additionally requires a
`CategoryKey`, a label, an order entry, and sunburst-key compatibility
(`rollupToSunburstInputs` leaf keys must match, or an L1 wedge click navigates to
a row that does not exist). That is a second, larger change on a surface with a
recorded history of partial un-folding (#1627, prevention-log #1845), and it
should not ride a fix that is otherwise contained.

**Stated rather than omitted**: if the operator wants the folded blockholders in
the L2 filer table, that is a follow-up, not a defect in this one.

## What this does NOT change

* `_PRIORITY_RANK`, `_reconcile_owner_once`, and every share figure in the pie.
  The overlay total is a restatement of shares the pie already counts once.
* The coverage banner's 5-state machine (#840/#923, settled) — untouched.
* `ownership_history` / the per-holder chart: the overlay is a point-in-time
  reconciliation artefact, not a series.

## Acceptance

1. Census run at full population, all five states reported, before/after.
2. Golden panel (`AAPL`, `GME`, `MSFT`, `JPM`, `HD`) on the live endpoint: each
   gains a `blockholders_restated` overlay naming the folded 13D/G filers, and
   `residual` / `concentration` / `sanity_checks` are **byte-identical** to the
   pre-change response.
3. An `absent_upstream` instrument emits **no** overlay.
4. `?category=blockholders_restated` CSV returns those holders; the documented
   sum invariant `Σ(non-`__memo:*`/`__dropped:*`) == shares_outstanding` still holds.
5. Revert probe: removing the max-not-sum rule fails a named test; removing the
   blockholders-slice exclusion fails a named test.
