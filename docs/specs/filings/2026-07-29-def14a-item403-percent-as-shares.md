# DEF 14A Item 403 — percent stored as a share count

Issue: #2163 (re-scoped; the original 23-accession wrong-table cohort moved
to #2160, which closes 19 of them by model change).

This ticket is the opposite failure to #2160. The table is selected
**correctly** — a genuine Item 403 table, genuine 5% holders — and the values
are read off the wrong columns.

## Source rule

**Schedule 14A Item 6(d)** (17 CFR 240.14a-101) requires the registrant to
furnish the information called for by Item 403 of Regulation S-K.

**17 CFR 229.403** (Reg S-K Item 403) prescribes the table for both subsections:

| # | 403(a) — >5% owners | 403(b) — management + group |
|---|---|---|
| 1 | Title of class | Title of class |
| 2 | Name **and address** of beneficial owner | Name of beneficial owner |
| 3 | **Amount and nature of beneficial ownership** | Amount and nature of beneficial ownership |
| 4 | **Percent of class** | Percent of class |

The load-bearing point: **columns 3 and 4 are DISTINCT columns with distinct
units.** Column 3 is an amount of securities — a count. Column 4 is a
percentage of the class. A value read out of column 4 and persisted as column 3
is not a rounding problem, it is a unit error: BlackRock's 17.4% became a
holding of 17.4 shares while its actual 6,236,345 shares were discarded.

**Rule 13d-3** (17 CFR 240.13d-3) defines "beneficial owner" as a person or
entity holding voting or investment power. This is what makes column 2 a name
and not a label — load-bearing for the Schedule 13D class below.

**17 CFR 240.13d-101 / 240.13d-102** prescribe the NUMBERED cover page for
Schedules 13D and 13G. Rows 7–11 are the sole/shared voting and dispositive
power lines and the aggregate-amount line; rows 1–6 and 12–14 are the
reporting-person, funding, citizenship and type-of-person lines.

**#1228 (settled in this parser):** a percent of class is bounded — ownership is
a fraction of a class — and `_parse_percent` has clamped to `[0, 100]` since
that ticket. That existing invariant is reused here as a ceiling, not
re-derived.

## The defect

`_resolve_columns` maps canonical columns by header index. A header row carrying
empty SPACER cells that its data rows do not carry is WIDER than the data, so
every resolved index after the first spacer is shifted right.

```text
0001308179-24-000672
column_headers = ['Name and address of beneficial owner', '', 'Number\n of shares', '', 'Percent\n of class*']
resolved       = (name=0, shares=2, percent=4)
row 0          = ['BlackRock, Inc. 1 50 Hudson Yards\n New York, NY 10001', '6,236,345', '17.4', '%']
stored         = BlackRock, Inc. … | shares=17.4000 | percent=NULL
```

`shares_idx=2` lands on the percent. `percent_idx=4` is off the end of a 4-cell
row. Critically `_parse_share_count('17.4')` **succeeds**, so the `if shares is
None` ragged-row recovery added in #2140 never fires and the real count at index
1 is never read.

`'17.4%'` in ONE cell was never this bug — `_parse_share_count` already rejects
it. The defect needs the value and its sign in SEPARATE cells, which is exactly
what HTML table rendering produces.

## Full-population verification

Sized **by re-parse, not by SQL proxy.** The stored-table proxy

```sql
SELECT count(*), count(DISTINCT accession_number)
FROM def14a_beneficial_holdings
WHERE shares IS NOT NULL AND shares <> trunc(shares) AND percent_of_class IS NULL;
--  166 rows | 68 accessions
```

is a **floor**: a percent of exactly `5` is stored as `5` shares and is
invisible to it. `scripts/census_def14a_percent_as_shares.py` traces the real
parse path over all 42,566 stored `def14a_body` payloads and inspects the
WINNING table's resolved `shares_idx` cell per row.

| population | accessions | rows |
|---|---|---|
| SQL proxy (fractional + percent NULL) | 68 | 166 |
| percent signature at `shares_idx` (census) | 176 | 741 |
| **the fix actually fires on** | **110** | **353** |

The gap between 741 and 353 is the `[0, 100]` ceiling doing its job: captions
such as `'2023-2025 PSU Shares Earned at 42% of Target (#)'` and `'Number and
Percentage of Shares Beneficially Owned'` contain a `%` but carry share counts
in the millions, and are left alone.

## Design

A share count is a whole number of securities; a value bearing a percent
signature under a column that is not column 3 is column 4's value.
`_shares_cell_percent_signature` classifies the cell resolved at `shares_idx`:

The split is **semantic vs positional evidence**, not "how many signals fired":

| signature | test | treatment |
|---|---|---|
| — | value `> 100` or `< 0` | never held back — cannot be a 229.403 column-4 percent (#1228) |
| `decisive` | caption at `shares_idx` names a percent and carries none of `_STRONG_SHARES_KEYWORDS` | the column states what it holds; held back and never restored |
| `weak` | next non-empty sibling cell is a lone `%` / `(%)`, **or** value is not an integer | circumstantial; held back, but **restored** if the row offers no whole-number alternative |

A lone `%` sibling was `marker` (decisive) in the first draft. Codex ckpt-2
falsified that: when an issuer renders the sign in its own column, a genuine
small holding sits immediately to its left — `[name, '50', '%', '0.1']` — and a
decisive reading drops the 50 shares. Column ORDER is not evidence about a
cell's meaning; the column CAPTION is.

Order matters as well as strength. The held-back cell is a **last-resort**
percent source, applied only after #2140's ragged-row scan, which accepts solely
cells carrying a `%` or the `*` marker and is therefore stronger evidence.
Multi-class tables carry several percent columns and `_resolve_columns`' `total`
tier matches `'Percent of Total Voting Rights'`; pre-empting the scan on
`0000950170-24-100030` (Richardson Electronics, 16 headers over 19-cell rows)
overwrote the real `Percent of Common Stock Class` value 14.8 with 98.1.
Found by arm 3 in A/B round 2.

Held back means `shares = None`, which lets #2140's existing recovery scan run
and find the real count elsewhere in the row. That scan already required
integrality; this change makes the PRIMARY parse consistent with it rather than
adding a new heuristic.

The `> 100` ceiling is load-bearing, not defensive. Without it a row rendered
`[name, '1,234,567', '%', '5.6']` — the sign cell BEFORE the percent value —
would read the sibling `%` as decisive, hold back a genuine 1.2M-share holding,
find no whole alternative (5.6 is fractional), and drop the count entirely.

`fractional` is deliberately weaker than `marker`: a fractional beneficial
holding is unusual but real (`FMR LLC` holds 15,072,586.57 on
`0000086312-26-000103`), so it must never be decisive on its own.

### Second class — embedded Schedule 13D/G cover pages

Proxies embed Schedule 13D/G cover pages as exhibits. The numbered layout parses
as a table whose "holder names" are the cover-page item labels and whose "share
counts" are the ROW NUMBERS:

```text
0001104659-17-023458
'SHARED VOTING POWER -0'                              shares=8
'SOLE DISPOSITIVE POWER 32,005,260 shares of Common'  shares=9
'SHARED DISPOSITIVE POWER -0'                         shares=10
```

The same label vocabulary also appears in **transposed** tables, where the
holders are COLUMNS and the rows are the power types — `0001308179-25-000114`
stored `Sole investment power` as a holder of 82,447,476 shares (that figure is
BlackRock's, read off the wrong axis). `investment` is therefore matched
alongside the cover page's own `dispositive`: Rule 13d-3 defines beneficial
ownership as voting **or investment** power and issuers paraphrase with it.

Note this predicate matches resolved **holder names**. #2160's D4 matches the
same vocabulary in **headers**, where it is a POSITIVE Item 403 signal
(229.403 column 3 legitimately subdivides into sole/shared voting power).
Different surfaces; the two do not contradict.

Neither the owner-identity test nor the address test rejects these — the
all-caps two-token shape (`SHARED VOTING POWER`) reads as a person name.
`_SCHEDULE_13D_COVER_LABEL_RE` matches the 240.13d-101/-102 cover-page item
labels, anchored at the start of the resolved holder name. 229.403 column 2 is a
beneficial owner (Rule 13d-3); a cover-page item label is not one.

## Not in scope

- **Wrong-table selection** (an Item 402 comp table or a capitalisation table
  winning over the real Item 403 table) is #2160. Where this fix fires on such a
  table it moves a junk share count to a junk percent — less wrong, still the
  wrong table, and #2160 removes the row entirely.
- **The window loop takes the FIRST qualifying window, not the best.** Untouched
  here; #2160 owns it.

## Verification

- Full-population A/B, three arms, per `.claude/skills/engineering/full-population-ab.md`.
  Control is the #2164 head (`2da7caa1`), which is what `main` becomes when
  PR #2170 merges. Metric is DISTINCT HOLDERS keyed as `holder_name_key`
  (`lower(trim(holder_name))`), never row count.
- Arm 3 audits `holder_role` drift, which arms 1 and 2 cannot see because both
  key on holder NAME.
- Parser version bumps to `def14a-v12` so the corpus re-drives through
  `_apply_def14a` with #2157's share-class sibling fan-out.
