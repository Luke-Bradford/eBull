# DEF 14A beneficial-ownership: column resolution + role classification

Issue: #2140. Parser: `app/providers/implementations/sec_def14a.py`.
Surfaced by #2121 (holder_role display overlay).

## Source rule

**Schedule 14A Item 6(d)** (17 CFR § 240.14a-101) is the proxy rule that puts
this table in a DEF 14A; it does so by **incorporating Reg S-K Item 403**
(17 CFR § 229.403), which governs the table's content and captions.

- **403(a)** — beneficial owners of more than 5% of any class of the
  registrant's **voting securities**. Our `principal` role.
- **403(b)** — directors, nominees, and named executive officers individually,
  **plus** the "all directors and executive officers as a group" aggregate.
  Our `director` / `officer` roles, plus `group`.
- The 403(b) group row is an **aggregate of its own constituents** — it is
  **non-additive** with the individual rows above it. It is therefore the one
  row that MUST remain distinguishable; summing a table that mislabels it as
  `officer` double-counts management.
- **The prescribed captions differ between the two subsections**, and both
  matter to column resolution:
  - 403(a) name column: **"Name and address of beneficial owner"**
  - 403(b) name column: **"Name of beneficial owner"**
  - both amount columns: **"Amount and nature of beneficial ownership"**

  The token `beneficial` appears in **every one of these**, and therefore
  carries **no discriminating signal**. The discriminating tokens are
  `name and address` / `name of` vs `amount and nature`.

## Standard-filing reuse check (edgartools)

Run empirically on our actual failing cases, not from the coverage matrix
(`.claude/skills/data-sources/edgartools.md` calls DEF 14A HTML extraction
"quality variable" — a starting point, not a verdict).

`edgar.proxy.html_extractor.extract_beneficial_ownership(tree)` (edgartools
5.30.2), against our stored `def14a_body` payloads:

| Filing | edgartools | ours (today) |
| --- | --- | --- |
| LOGI `0001032975-26-000037` | `None` | 15 rows, every name numeric |
| MKTX `0001193125-26-191601` | `None` | 26 rows, every name numeric |
| UBER `0001308179-26-000125` | `None` | 19 rows, every name numeric |
| HLF `0001213900-26-029131` | `None` | 18 rows, names newline-split |
| CYH `0001193125-26-140269` | **26 rows, names + percent correct** | 24 rows, names numeric, **percent all `None`** |

**Verdict: do NOT adopt *for this ticket*.** It returns nothing on 4 of the 5
filings that motivate the work, so it cannot be the repair path for these
defects. This is explicitly **not** a general claim about its DEF 14A coverage —
that would need a full-corpus run, which this ticket does not perform. It is
retained as a **cross-source oracle**: CYH is the cross-source verification case
in §Verification, and its output is what revealed defect D3 below (percent
loss), which was not in the original report.

## Defects

### D1 — column-index collision (3,209 rows = 13.4% of shares-bearing)

`_resolve_columns` (`sec_def14a.py:620`) resolves `name_idx` in the **same pass**
as the others, matching on `"name" in lower or "beneficial" in lower`. Because
`beneficial` is shared by both Item 403 captions, a blank name caption makes the
first `beneficial` header the **shares** column, so `name_idx == shares_idx` and
the share count is persisted as `holder_name`.

Observed header tuples (traced against the live function on stored bodies):

| Filing | resolved headers | `(name, shares, percent)` |
| --- | --- | --- |
| LOGI | `('', 'Number of Shares Owned (1)', 'Shares that May be Acquired…', 'Total Beneficial Ownership', 'Total as a Percentage…')` | `(3, 3, 4)` |
| MKTX | `('', '', 'Number of Shares Beneficially Owned', '', 'Percentage of Stock Owned')` | `(2, 2, 4)` |
| UBER | `('', '', 'Shares Beneficially Owned')` | `(2, 2, 2)` |
| CYH | `('', '', 'Shares Beneficially Owned (1)')` | `(2, 2, 2)` |
| HLF | `('Name\n of beneficial owner', '', 'Amount\n and nature…', '', 'Percentage\n ownership (1)')` | `(0, 2, 4)` ✅ |

### D2 — two-row header not promoted (UBER / CYH class)

UBER and CYH resolve to a **3-cell** header row whose real column labels live on
the **next** row:

```text
row 0 (spanning): ('', '', 'Shares Beneficially Owned')
row 1 (real):     ('Name of Beneficial Owner', '', 'Shares', '', '% of Shares Outstanding')
```

`_parse_table_html` already promotes a row-1 sub-header, but only when
`_looks_like_subheader` fires — and that helper requires one of
`sole | shared | total | voting | dispositive`. UBER/CYH row 1 contains
`name | shares | percent`, so it is **not** promoted, stays a data row, and
column resolution is left with the useless spanning row. This is why
`name_idx == shares_idx == percent_idx == 2` for both.

### D3 — percent lost (NEW; found by the edgartools oracle, not in the report)

A collapse to a single index also destroys `percent_of_class`: CYH stores
`percent_of_class = NULL` on every row, where the same filing yields
`8.4 / 6.9 / 6.9 / 6.0 / 5.6 / 5.2` via edgartools. The `percent_idx` fallback
`len(headers) - 1` is wrong whenever the header row is **narrower than the data
rows** (D2's spanning-header case).

### D4 — role precedence inversion (915 of 1,530 group rows = 60%)

`sec_def14a.py:903`:

```python
role = current_role or _detect_inline_role(holder_name)
```

`_detect_inline_role` already returns `"group"` for any name containing
`as a group` (`:690`) — it simply never runs once a section heading has set
`current_role`. So the Item 403(b) aggregate inherits `officer` from the
management heading above it. The same inversion strands 403(a) 5%-holders that
follow a management heading: HLF stores `The Vanguard Group`,
`The Baupost Group, L.L.C.`, `BlackRock, Inc.`, `Renaissance Technologies LLC`
all as `officer` (1,226 institution-suffix rows full-pop).

The correct shape **already exists 14 lines below** — the ESOP override at
`:917`, whose comment states the principle: *"name-pattern detection wins over
section-derived role"*.

Secondary: `_ROLE_HEADING_PATTERNS` (`:655`) is first-match-wins with the
`group` pattern **last**, so pattern 1 (`directors?.*officers?`) matches every
"all directors and executive officers as a group" heading first and the `group`
pattern at index 7 is unreachable.

### D5 — un-normalised newlines split holders (704 rows / 117 instruments)

`_INLINE_WHITESPACE_RE = [ \t\r\f\v]+` deliberately excludes `\n` (kept for the
Item 402(c) SCT name/title split). `holder_name_key` is
`lower(trim(holder_name))` (`sql/116:110`) — `trim` does not touch interior
whitespace, so `'Michael\n O. Johnson'` and `'Michael O. Johnson'` are **two
different identities**.

Full-population: **704 newline-bearing rows across 117 instruments**, of which
**51 are group rows**; normalising collapses them into **163 duplicate
clusters**.

Correction to the 07-24h diagnosis: the HLF pairs are **not** intra-filing
duplicates. They are different accessions/period_ends
(`0001213900-26-029131` @ 2025-12-31 newline-split vs `0001213900-25-022830`
@ 2024-12-31 clean). The damage lands in `ownership_def14a_current`, whose PK
is `(instrument_id, holder_name_key, ownership_nature)` — so HLF currently shows
**14 people twice** and **two group rows**, one stale. The "truncated group row"
claim in that comment does **not** reproduce; the stored text is complete.

### D6 — a parser fix alone cleans nothing (the real blocker)

`record_def14a_observation` (`ownership_observations.py:1136`) is
`INSERT … ON CONFLICT (instrument_id, holder_name_key, ownership_nature,
period_end, source_document_id) DO UPDATE`. Re-parsing an accession under a
fixed parser writes rows under the **corrected** keys and leaves every broken
key live. `refresh_def14a_current`'s MERGE prunes with
`WHEN NOT MATCHED BY SOURCE THEN DELETE`, but its source set is
`ownership_def14a_observations WHERE known_to IS NULL` — which still contains
the broken rows. So all 3,209 + 704 survive a rewash.

## Design

### 1. `_resolve_columns` — exclusion-ordered, Item-403-token discriminated

Resolve in dependency order, each step excluding already-claimed indices:

1. `shares_idx` — unchanged tiered search (`total` > `amount and nature` >
   `shares beneficially`/`shares owned` > `shares`/`number`/`amount`).
2. `percent_idx` — header match on `percent`/`%`, **excluding `shares_idx`**.
3. `name_idx` — header match on Item 403's *name-side* tokens only:
   `name and address`, `name of`, `beneficial owner`, `stockholder`,
   `shareholder`, `holder` — **never bare `beneficial`** — excluding
   `shares_idx` and `percent_idx`. Fallback: column 0.

### 2. `_looks_like_subheader` — widen the label vocabulary, safely

Naive substring widening is **unsafe**: `Named Executive Officers and
Directors` contains `name`, contains no multi-digit run, and is exactly the
single-cell section-heading row that sits directly under UBER's spanning
header — substring matching would promote it to the header row and destroy the
section boundary. Codex ckpt-1 caught this.

Three guards together:

1. **Word-boundary** matching (`\bname\b`, `\bpercent\b`, `\bnumber\b`,
   `\bshares?\b`, `\bowner\b`, `%`), never substring — `\bname\b` does not
   match `Named`.
2. **At least two distinct label classes** present in the row
   (name-class / amount-class / percent-class). A genuine column-label row
   labels more than one column; a section heading names one thing.
3. The existing **all-text guard** (no multi-digit run in any cell) is
   retained but is explicitly *not* load-bearing on its own — it is false for
   any text-only heading row, which is precisely the failure mode above.

The existing `common`/`preferred` false-positive finding stays respected
(neither is added back), and the `sole|shared|total|voting|dispositive`
keywords remain, counting as the amount-class.

### 3. Structural guard — a nameless `holder_name` can never persist

Stated as a positive invariant, not a numeric blacklist: **a holder name must
carry name evidence** — an alphabetic run of 2+ characters after cleaning.
A `^[\d,.\s]+$` test would miss `%`, `<1%`, `*`, `—`, and footnote-only cells
that clean to punctuation (Codex ckpt-1 MED).

Reuse the predicate already in this module: `_looks_like_name_cell`
(`sec_def14a.py:1208`) is exactly this test (`re.search(r"[A-Za-z]{2,}")` plus
year/blank rejection) and is already proven on the Item 402(c) path.

In the row loop, after resolution: if the chosen name cell fails
`_looks_like_name_cell`, fall back to the leftmost cell in that row that
passes. If none does, **drop the row** — an unnamed holder is not a holder.
This is the invariant that makes D1-class regressions impossible regardless of
future header shapes.

### 4. Role precedence — the group aggregate overrides section context

Scope note: this is a **group-only** override, not a general "name evidence
beats section context" rule. Every other `_detect_inline_role` signal keeps
its current precedence (section context first), because a section heading is
genuinely better evidence than an inline job title for individuals — only the
403(b) aggregate is a row whose identity the section cannot express.

Mirror the ESOP override's shape:

```python
role = current_role or _detect_inline_role(holder_name)
if "as a group" in holder_name.lower():
    role = "group"          # Item 403(b) aggregate — non-additive
```

**Remove** the `group` entry from `_ROLE_HEADING_PATTERNS` rather than
reordering it. Codex ckpt-1 caught the trap: `_detect_role_heading` sets
`current_role` for **all subsequent rows**, so making the `group` pattern
reachable would turn one "all directors and officers as a group" heading into
a sticky `group` context that mislabels every row after it — strictly worse
than today's unreachable-pattern state. Per Item 403(b) the aggregate is a
**row**, not a section, so it belongs in inline detection only. The 615 rows
currently tagged `group` already come from `_detect_inline_role`, so removing
the heading pattern loses nothing.

Institution-suffix rows (D4's 1,226) are **out of scope for a name-suffix
heuristic** — `The Vanguard Group, Inc.` following a management heading is a
section-boundary problem, and inferring `principal` from an `LLC`/`L.P.`
suffix is exactly the first-principles reasoning the engineering rules forbid.
What this spec fixes is the precedence inversion; the residual institution
misclassification is measured in the A/B and, if material, filed separately.

### 5. Newline normalisation — narrowest possible blast radius

Normalise **inside `_clean_holder_name`** (the beneficial-ownership name entry
point), not in `_strip_inline_html`. Every SCT consumer of
`_clean_holder_name` (`_split_name_position`, `_normalize_first_cell`,
`_looks_like_name_cell`) already flattens `\n` itself, so Item 402(c) output is
unchanged — asserted by the A/B in §Verification.

### 6. Accession-scoped supersession (unblocks D6)

Before re-recording a filing's holders, supersede that filing's prior def14a
observation rows (`known_to = NOW()`), then insert what the current parse
produces.

**Supersede, not DELETE** — invariant **I6** (`data-engineer` skill) is
"soft-delete via tombstones, never hard-delete observations". The #953 13F
precedent (`rewash_filings.py:1362-1385`) used a hard DELETE, but its stated
reason is purely mechanical: *its* writer's `ON CONFLICT DO UPDATE` never
cleared `known_to`, so a re-asserted row would have stayed invisible to the
`_current` MERGE forever. That is a gap to close, not grounds to destroy
observations. So this change also adds `known_to = NULL` to
`record_def14a_observation`'s `ON CONFLICT DO UPDATE`: every holder the new
parse still emits is revived in the same transaction, and only the rows it no
longer emits stay tombstoned — with their audit history intact. Verified that
every real reader of `ownership_def14a_observations` already filters
`known_to IS NULL` (`refresh_def14a_current`, `refresh_def14a_current_batch`,
`ownership_history`); the only unfiltered reads are the `MAX(ingested_at)`
watermark queries, which should span superseded rows.

**Placement: inside `_record_def14a_observations_for_filing` itself**, not at
the rewash call site. Codex ckpt-1 flagged that stale-key reassertion can occur
at *any* write locus; putting the replace in the shared writer covers all of
them by construction (`def14a_ingest.py:1018` first-ingest and
`rewash_filings.py:641` rewash are the only two callers, verified by grep).
On first ingest the delete is a no-op.

**Scope: `(instrument_id, source_document_id)`, NOT accession alone.** The
writer is called **once per share-class sibling** for the same accession
(`def14a_ingest.py:995-1024` fans out over `siblings`), so an
accession-only delete would make each sibling's write wipe the previous
sibling's rows. Also filtered on `source = 'def14a'`.

Runs in the same transaction as the re-insert, capturing `RETURNING` so an
instrument whose rows vanish entirely still gets `refresh_def14a_current`
called and its stale `_current` rows pruned by the MERGE's
`NOT MATCHED BY SOURCE` arm.

The stale comment at `def14a_ingest.py:1011-1014` ("record_def14a_observation
is itself UPSERT so re-ingest of the same accession (parser bump) refreshes
existing rows in place") is the assumption this defect falsifies — it holds
only while the parser does not change the **name**, which is the key. Corrected
in the same change.

This makes the def14a re-ingest **idempotent and self-cleaning**, which is what
turns the rewash into a real backfill rather than an additive one.

## Defects found by the full-population A/B (not in the original report)

Six full-corpus runs. Each surfaced mechanisms the five-filing panel passed
over; the panel was green on every run that was not yet clean.

- **D7 — Item 403 has TWO tables.** 403(a) >5% owners and 403(b) management are
  routinely separate tables, and only one was ever read. Which one survived
  turned on incidental header wording, so any scoring change flipped it
  (0001193125-26-119922: 14 rows → 0). All qualifying tables in the winning
  window are now collected, deduped on **holder identity** — not
  `(name, shares, percent)`, which kept a filing's *breakdown* table and put
  all 16 of its people in twice (0000080661-25-000018).
- **D8 — Item 402 award tables outscoring Item 403.** Both families use
  "number of shares", so a 402(d)/(f) grants table can win on keyword weight —
  and folding unicode spaces (D11) made those captions start matching, taking
  Hershey from 26 holders to 7 rows of grant data. Disqualified outright.
- **D9 — share count inflated 10×.** `'52,606,862 1'` → **526,068,621**: an
  unbracketed footnote survived and the space was collapsed into the number.
  Pre-existing on `main`.
- **D10 — merged Item 403 caption.** "Amount and Nature of Beneficial Ownership
  **and Percent of Class**" is ONE column holding the share count; percent-first
  resolution stole it and shares never resolved.
- **D11 — literal U+00A0 in captions.** `_NBSP_RE` only caught the `&nbsp;`
  *entity*, so "Amount\xa0and\xa0Nature" never matched its own prescribed
  caption and a "Common Shares of <Issuer>" title column won the shares tiering,
  putting `shares_idx` on the name column.
- **D12 — address fragments as holders.** Item 403's column is "Name **and
  address**"; when the address is split across sibling `<tr>` rows each
  continuation line becomes a row with a real share count
  (`'c/o Dolan Family Office'` @ 11,484,408 / 100%).
- **D13 — typed table additive on rename.** Same shape as D6 but on
  `def14a_beneficial_holdings`, which the rollup / drillthrough / drift readers
  use. `rewash_filings` deleted those rows; the manifest re-drive that the
  v6→v7 bump triggers did not. (Codex pre-push.)

### Measuring the A/B correctly

Row count is the WRONG metric twice over, and both errors pointed at fixes that
would have made the parser worse:

1. A row-count drop is often `main` losing **garbage** — 48 accessions where it
   was parsing an Item 402 award table (dates as holder names), 23 with
   majority-garbage names. Chasing the count would have re-admitted them.
2. A drop is also the **identity dedup working** (61 accessions) — which is what
   D5 exists to do.

The metric that matters is **distinct holders lost**. Under it the final run
shows **zero**. Gains must be inspected too: losses were clean before D12 was
found, and D12 only appears on the gain side.

## Verification

### Full-population A/B (offline — no EDGAR fetch)

42,505 `def14a_body` payloads are stored in `filing_raw_documents`, so the
whole corpus re-parses in-process. Harness re-parses every body under `main`
and under the branch and diffs:

- numeric `holder_name` count: **3,209 → expect 0**
- newline-bearing `holder_name`: **704 → expect 0**
- `as a group` rows not tagged `group`: **915 / 1,530 → expect 0**
- rows with `shares IS NOT NULL` but `percent_of_class IS NULL`: expect a
  **decrease** (D3)
- Item 402(c) SCT rows: expect **byte-identical** (D5 blast-radius proof)
- total row count / per-accession row deltas: enumerated, not summarised —
  any accession losing rows is inspected, not averaged away.
- **Promoted-row audit** (Codex ckpt-1): for every accession where
  `_looks_like_subheader` promotes row 1 to the header, log the accession, the
  promoted row's text and width, the data-row width, and the resulting
  `(name, shares, percent)` map. Reviewed as a list — the claim "the guards
  prevent a section heading being promoted" is verified against the whole
  corpus, not asserted from the five-filing panel.

### Backfill (ETL clause 10)

`uv run python scripts/rewash.py --kind def14a_body` (`PYTHONPATH=.`) — offline
re-parse of all 42,505 bodies, no rate-limited EDGAR drain. `--dry-run` first.

### Operator-visible (ETL clauses 8, 9, 11)

- Panel: **LOGI / MKTX / HLF / UBER / CYH** — the five filings that carry the
  defects, not the generic AAPL/GME/MSFT/JPM/HD panel.
- Endpoint: `/instruments/{symbol}/ownership-rollup` after backfill; confirm
  named holders replace bare numbers and the group row carries `group`.
- Cross-source: **CYH** against edgartools' independent extraction
  (26 rows, Apollo 11,838,609 / 8.4%, Blackrock 9,750,502 / 6.9%) and against
  SEC EDGAR direct for one row.
