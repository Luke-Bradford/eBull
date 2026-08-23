# Surface the thesis subject-identity verdict to the operator (#2306)

Status: spec, 2026-08-23. Ticket: #2306. Guard named by: #2842.

## What is already true (measured, not inherited)

#2306 was filed when no discriminator existed. Two later tickets shipped one:

- **#2431** — `app/services/thesis_subject_identity.py::memo_names_subject`. Positive
  containment ("does the memo name its own instrument?"), which sidesteps all three
  "first parenthesised token" discriminators #2306 falsified. Enforced at write time in
  `_validate_writer_output` (`app/services/thesis.py:1743`), riding retry-once.
- **#2436** — `sql/332` stores the verdict on the row
  (`subject_identity_ok` / `_rule_version` / `_checked_at`, all-set-or-all-NULL CHECK),
  `ensure_subject_identity_verdicts` backfills it at lifespan, and every deterministic
  consumer fails closed on `IS NOT TRUE`: `portfolio.py:455`, `scoring.py:1291`/`:1649`,
  `entry_timing.py:366`, `alerts.py:845`, `reporting.py:649`.

So this ticket's decision-path half is closed. Its last section is not:

> **Also open: the 19 stored bad memos** — They are rendered to the operator today.

### Full-population measurement, dev DB, 2026-08-23

```sql
SELECT count(*), count(*) FILTER (WHERE subject_identity_ok IS FALSE),
       count(*) FILTER (WHERE subject_identity_ok IS NULL) FROM theses;
-- 3714 | 1512 | 0
```

```sql
WITH latest AS (
  SELECT DISTINCT ON (instrument_id) instrument_id, prompt_version, subject_identity_ok
    FROM theses ORDER BY instrument_id, created_at DESC, thesis_version DESC)
SELECT prompt_version, count(*), count(*) FILTER (WHERE subject_identity_ok IS FALSE)
  FROM latest GROUP BY 1 ORDER BY 1;
-- v3 1/0 · v4 88/0 · v5 90/70 · v6 8/8 · v7 295/0   → 78 of 482 latest rows quarantined
```

⚠ The `DISTINCT ON` tie-break is `thesis_version DESC`, matching `_LIBRARY_SQL:517` and
`portfolio.py`, not an ad-hoc `thesis_id DESC`. Checked rather than assumed — the two
orderings pick a different row on **0** of 482 instruments here, so the 78 is stable
under either, but the query stated is the consumers'.

⚠ **Reconciling the four numbers, because three of them are in this ticket's history and
they measure different sets.** 1,512 = every stored row the current rule refuses.
78 = the subset that is the LATEST row for its instrument, i.e. what a page renders.
178 = `sql/332`'s equivalent latest-row count on 2026-08-12, since superseded by v7
regenerations. 19 = #2306's own 2026-08-05 count of a *narrower* class — memos whose
first parenthesised token resolves to a different real symbol. The stored rule is
stricter than that class: it refuses any memo that never names its subject, of which
naming a different company is one case. All four are unusable to a consumer, which is
why one verdict covers them; they are not the same measurement and must not be
substituted for one another.

Latest-row samples, so the class is concrete rather than statistical:

| symbol | prompt | stance | base_value | memo opens |
| --- | --- | --- | ---: | --- |
| AA | v5 | avoid | — | "SQQQ (ProShares UltraPro QQQ) is a leveraged inverse ETF" |
| ACA.US | v6 | buy | 150.00 | "### 企业名称：Coca-Cola Company (KO)" |
| AAPL | v5 | buy | 258.50 | "[Company] exhibits strong fundamentals…" |
| AGCO | v6 | buy | 100.00 | "The company demonstrates strong fundamentals…" |

## The defect

`grep -rn subject_identity_ok frontend/src` returns **nothing**. The field is on both
Pydantic response models (`app/api/theses.py:207` detail, `:280` list) and in both
SELECTs (`_THESIS_COLUMNS:334`, `_LIBRARY_SQL:488`/`:515`) — the wire carries it and the
frontend never declared it.

So on the 78 instruments above, today:

- `SummaryStrip.tsx:271-281` renders an always-visible `Thesis: BUY 85%` chip.
- `ThesisPane.tsx:268-320` renders the bear/base/bull/buy-zone band and the memo.
- `ThesesPage.tsx:359`/`:376` renders stance + buy zone in the library.

Every one of those is a verdict the deterministic layer has already refused. This is a
sharper form of the repo's recurring writer-with-no-reader shape: the readers exist in
*every* decision path and are missing only on the surface a human reasons from, so the
operator and the engine disagree about the same row with nothing on screen saying so.

⚠ Not hypothetical downstream: `sql/332`'s header records 14 EXIT recommendations that
had already fired "Valuation target reached" against a `base_value` written about a
different company. That path is now closed in code; the screen still shows the band.

## Decision — annotate, do not hide, do not regenerate

Three options existed and this is decided here rather than escalated (the facts to settle
it are all above):

- **Regenerate** — infeasible and not merely slow: `thesis_refresh` is parked
  (`llm_model_writer = 'parked-2855'`, #2855) and measured at 651 s/thesis, so 1,512 rows
  is ~11 days of a starved box. It also loses the evidence base.
- **Hide the row** — contradicts `docs/settled-decisions.md:147` ("do not overwrite prior
  thesis rows") and `app/api/theses.py:202-206`, which states the row stays visible
  *because* it is the truthful record. Hiding also makes the page lie in the other
  direction: it would read as "no thesis exists", which is a different false state.
- **Annotate** ← chosen. The row keeps rendering; the screen states the verdict the
  engine already reached, in the same words (`thesis_quarantined`).

## Design

### 1. One predicate, mirroring the Python one

`frontend/src/lib/thesisQuarantine.ts`:

```ts
export type ThesisSubjectState = "usable" | "unnamed_subject" | "unchecked";
export function thesisSubjectState(ok: boolean | null | undefined): ThesisSubjectState;
export function isThesisQuarantined(ok: boolean | null | undefined): boolean;
```

`isThesisQuarantined` is `ok !== true` — the exact complement of
`thesis_subject_identity.is_thesis_usable` (`row.get(...) is True`). `undefined` (field
absent from an older payload or a partial test fixture) therefore reads as quarantined,
which is the fail-closed direction and matches how NULL is treated server-side.

⚠ The `false` state is named `unnamed_subject`, **not** `misattributed`. `false` records
only that the memo never named its own instrument; it does not establish that some other
company was positively identified. Naming the state for the stronger claim would restate,
in the type system, exactly the conflation this spec's measurement section warns against.

`unnamed_subject` (false) and `unchecked` (null/undefined) differ only in copy: "never
names its subject" vs "not yet checked". Both refuse.

⚠ **The predicate alone is not sufficient on the library.** `ThesisLibraryItem` gives a
row to held instruments that have **no thesis at all**, with every thesis field null
(`app/api/theses.py:235-239`) — so `ok !== true` is true there for the trivial reason
that there is nothing to check. Marking those "quarantined" would invent a defect. The
library marker is therefore gated on `thesis_id !== null` as well; `ThesisPane` and
`SummaryStrip` need no such gate because both already early-return on a null thesis.

### 2. One banner component

`frontend/src/components/theses/ThesisQuarantineBanner.tsx`, modelled on
`OwnershipCoverageBanner.tsx` — `role="status"`, `data-subject-state`, glyph +
headline + body, dark-mode pair per `design-system.md`.

Copy names the machine-readable reason so the screen and the logs agree:

> ⊘ **Thesis quarantined — subject identity failed.** This memo never names AAPL, so its
> figures may describe a different company. The deterministic layer refuses it
> (`thesis_quarantined`) — portfolio, scoring, entry timing, alerts and reporting; it is
> shown for evidence only.

⚠ The copy claims exactly what the stored rule decided ("never names its subject") and
hedges the consequence ("may describe a different company"). It must not be tightened to
"is about a different company", which the verdict does not establish. The consumer list
is the census verified above, not a paraphrase — if a consumer is added or drops the
guard, this sentence is wrong and the census in this spec is where to correct it.

### 3. Wiring (frontend only — no backend or schema change)

| file | change |
| --- | --- |
| `api/types.ts` | `subject_identity_ok: boolean \| null` on `ThesisDetail` + `ThesisLibraryItem` |
| `components/instrument/ThesisPane.tsx` | banner first in the body; band marked refused and its derived conclusions suppressed |
| `components/instrument/SummaryStrip.tsx` | stance chip takes the refused tone + `⊘` when quarantined |
| `pages/ThesesPage.tsx` | `⊘` marker beside `StanceBadge`, gated on `thesis_id !== null` |

`VerdictTab.tsx`, `ResearchTab.tsx` and `DensityGrid.tsx` all render the memo *through*
`ThesisPane`, so they inherit the banner and are not touched.

⚠ The TS fields are **required** `boolean | null`, not `?: boolean | null`. Both models
declare `subject_identity_ok: bool | None = None` and Pydantic serialises the default, so
the key is always on the wire; `api-shape-and-types.md` makes asymmetric nullability the
drift bug to avoid. Optional would also let a page silently omit the field and typecheck
clean. The predicate still accepts `undefined` — that is defence for hand-built test
fixtures, not a wire state the types admit.

### 3a. Derived conclusions are suppressed; stored numbers are not

"Annotate, do not hide" applies to the *record*. It does not extend to conclusions this
component computes on top of a refused record, which read as live analysis rather than as
evidence:

- `upsideToBase` — the `+12.4%` chip beside Base.
- `outsideZone` — "Current price is outside the buy zone — entry conditions not met at
  market."

Both are suppressed when quarantined. Bear/base/bull/buy-zone themselves keep rendering,
inside the refused band, because they are what the writer actually produced and this
surface is the evidence base for them.

### 4. The invariant that makes this a safety surface, not decoration

**The banner and the memo must be inseparable.** `safety-state-ui.md` forbids deriving a
safety banner from a refetchable async value, because a refetch nulls `data` and the
banner vanishes while the page still reads dangerous. That failure mode is structurally
absent here only because both are read off the *same* `thesis` object, and `ThesisPane`
returns `null` when `thesis === null && !errored` — so on a refetch the memo and the
banner disappear together. That is a property to assert, not to assume: a test renders
the pane across the state matrix and fails if any state shows `memo_markdown` without
the banner.

## Tests

- `lib/thesisQuarantine.test.ts` — table over `true` / `false` / `null` / `undefined`.
- `ThesisPane.test.tsx` — banner present on false/null/undefined, absent on true; the
  inseparability assertion above; band marked refused; `upsideToBase` and the
  outside-zone sentence absent when quarantined and present when not.
- `SummaryStrip.test.tsx` — chip carries the refused marker; unquarantined chip unchanged.
- `ThesesPage.test.tsx` — marker on a quarantined row, absent on a clean one, and
  **absent on a held row with `thesis_id === null`**, which is the case the bare
  predicate gets wrong.

Accessibility: the `⊘` glyph is `aria-hidden` everywhere and always accompanied by text
the assistive layer can read — the banner's headline on the pane, and an `sr-only`
"quarantined" beside the library marker. A glyph with only a `title` is not a label.

## Source rule

None governs this: it is a UI treatment of a verdict whose rule (`#2431`) is already
fixed and version-hashed. The discipline that binds is full-population verification of
the *claim*, which is the two queries above run over all 3,714 rows.

## Out of scope

- Changing `memo_names_subject` itself. Its version hash covers the whole file, so any
  edit rewrites all 3,714 stored verdicts (`thesis_subject_identity.py` docstring).
- Regenerating or deleting rows.
- #2306's original A/B of the v6 prompt anchor, which stays unpowered at these rates.
- **A rule-version freshness check in the UI.** Raised at Codex ckpt-1 and rebutted:
  `ensure_subject_identity_verdicts` runs at LIFESPAN and re-decides every row whose
  `subject_identity_rule_version IS DISTINCT FROM` the current one, so a verdict issued
  under a superseded rule cannot survive a boot. Re-deriving that on the client would be
  a second copy of the rule with no way to stay in step.
- **`DiffBlock` against a quarantined PREDECESSOR.** A clean latest thesis can carry a
  diff whose "was" side came from a refused row. Real, and not addressed here: the diff
  is computed server-side in `_fetch_diffs` on an explicit `thesis_version - 1` lookup
  that does not consult the verdict, so the fix belongs in that query, not on the screen.
  Noted on the PR rather than ticketed, per the loop's no-new-audit-ticket rule.
