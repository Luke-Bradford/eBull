# DEF 14A SCT name/title split — role-boundary-first (#2097)

Status: proposal (unshipped). Branch `fix/2097-def14a-sct-name-wrap`.

## Problem

`app/providers/implementations/sec_def14a.py::_split_name_position` splits the
SCT first cell ("Name and Principal Position", Item 402(c)(2)(i)) into
`(executive_name, principal_position)` using a **newline-first** rule
([:1168](../../../app/providers/implementations/sec_def14a.py#L1168)): line 1 =
name, remainder = title. But a `\n` in an SCT first cell is a *render wrap* at an
arbitrary point, not a name/title delimiter. It falls:

- **mid-name** → name truncated to the first token (Alphabet `0001308179-25-000511`:
  `'Sundar\n Pichai \n Chief Executive Officer…'` → name `'Sundar'`). This is
  #2097 Class A (1,383 rows / 169 acc, full-pop).
- **mid-title** → name absorbs the title
  (`'T. Wilson Eglin Chief Executive Officer\nand President'` → name
  `'T. Wilson Eglin Chief Executive Officer'`). This is the v4 "…President" leak
  subclass of #2097 Class B.

Same root cause, both faces. #2097 hypothesised a *cross-physical-row* wrap and a
"lookahead-merge of the next row" — falsified: the wrap is **intra-cell**.

## Source rule

- **What the reg fixes (documented):** SEC Reg S-K **Item 402(c)(2)(i)**
  (17 CFR § 229.402(c)(2)(i)) — SCT column (i) is a single "Name **and**
  Principal Position" column holding the name followed by the title. That is the
  extent of the *documented* rule.
- **What is parser POLICY (inference, not SEC-mandated), stated plainly per
  Codex ckpt-1:** that the name/title boundary is the *onset of the position
  title* and that an HTML line break inside the cell is a *render artifact*, not
  a semantic delimiter. The reg does not mandate either; we adopt them as parser
  policy, corroborated by (a) the standard tooling implementing the same shape
  and (b) full-population evidence (below) — not claimed as a documented SEC
  boundary rule.
- **Corroboration (edgartools 5.30.2)**: `edgar.proxy.html_extractor._split_name_title`
  ([:551](../../../.venv/lib/python3.14/site-packages/edgar/proxy/html_extractor.py#L551))
  flattens `\s` (incl. `\n`) to spaces *first*, then splits at a title keyword
  ([:570](../../../.venv/lib/python3.14/site-packages/edgar/proxy/html_extractor.py#L570)),
  and returns the whole cell as name when no title keyword is found
  ([:581](../../../.venv/lib/python3.14/site-packages/edgar/proxy/html_extractor.py#L581)).
  The flatten-then-role-split shape is the standard approach.
  **Not adopted wholesale**: edgartools then *comma*-splits before its keyword
  split, so on multi-clause NEO cells it leaks the first title phrase into the
  name (verified: `'Sundar Pichai Chief Executive Officer'`). Our role-keyword
  split is strictly better here. See edgartools skill G16 + research ticket #2099
  (Item 402(v) Pay-vs-Performance dimensional iXBRL as a structured NEO-name
  cross-check).

## Change

1. **`_POSITION_ROLE_RE`**: add `executive` to the leading-modifier alternation
   (so "Executive Chairman"/"Executive Vice President" are recognised as titles,
   fixing both the name/title split *and* `_position_only_cell`'s title
   detection); make vice-chair hyphen-tolerant (`vice[-\s]+chair`, for
   "Vice- Chairman").
2. **`_split_name_position`**: flatten `\n` → space; split at the first
   `_POSITION_ROLE_RE` match (the semantic boundary); if none, the whole
   flattened cell is the name (no title present). Drops the newline branch
   entirely — `\n` becomes plain whitespace. The whole-cell fallback only ever
   *preserves* the name (never truncates) — consistent with the parser's
   "keep the legal name unless a strong delimiter" posture.

No schema change. No new dependency. `principal_position` free-text unchanged (v1).
**No post-hoc name mutation** — an earlier `_trim_trailing_title` step was
dropped after Codex ckpt-1: full-pop showed the `executive`-modifier regex fixes
the compound-title leaks *at the split boundary* (identical result with/without
the trim), so a trailing-vocab trim that could eat a real title-vocab surname
("Bank", 3-token "Robert A. Bank") is unnecessary and not adopted.

**Rejected alternative (Codex ckpt-2):** keeping a newline-split fallback for
"name\\ntitle" cells whose title lacks a role keyword. Rejected on evidence: (1)
the cited intra-cell pattern has **0 observed instances** in the 169-accession
full-pop — the one over-glue (`Thomas Leonardo Global Head of Accident and
Health`) is a *cross-row* concatenation (real cell = `'Thomas\\n Leonardo'`, a
wrapped name; the title rides a separate physical row — #2100 Class 4), which a
same-cell newline fallback cannot touch; (2) any newline-split gated on "line-1
is a plausible name" also **truncates** the observed wrapped-name pattern
`Mary Jane\\nSmith` → name `Mary Jane` (first+middle on line 1, surname on line
2) — a *harmful* wrong-name regression, strictly worse than a non-harmful
over-glue. The whole-cell fallback errs toward preserving the full name, per the
parser's posture and Codex ckpt-1's own anti-name-mutation guidance.

**Accepted residual risk (Codex ckpt-1, near-zero):** the `executive` modifier
shifts the split left when a name token `Executive` precedes a role word
("Jane Executive Director" → `Jane` / `Executive Director`). "Executive" is not
a real surname; no such case in the 169-accession population.

## Full-population verification (evidence, not proof — Codex ckpt-1)

Re-parsed all **169** Class A accessions' stored `def14a_body` (current vs
patched splitter, through the real `parse_summary_compensation_table`), distinct
`(name, position)` pairs. This is the Class A population + the standard panel;
it does **not** exhaustively cover every #2088 stacked or #2094 wrapped-title
layout, so those get targeted fixtures (below) rather than resting on this scan:

| signal | current | patched |
|---|---|---|
| single-token `executive_name` | 563 | **25** (−96%) |
| `executive_name` ends in a title word | 8 | **3** (−62%) |
| NEW title-ending names introduced | — | **0** |
| over-glued name (>6 tokens) | — | **1** |
| panel AAPL / GME / MSFT / JPM / HD (latest DEF 14A) | — | **0 rows changed** |

- **Accepted residual (1 over-glue)**: `'Thomas Leonardo Global Head of Accident
  and Health'` — a genuinely non-lexicon title ("Global Head of…", no role
  keyword) the whole-cell fallback keeps intact. Non-harmful (name present, comp
  correctly attributed); 1 case in 169 vs ~530 truncations fixed.
- **Out of scope → separate ticket**: the 25 remaining single-token names are
  *distinct* classes the intra-cell fix cannot address — cross-**physical-row**
  name wrap (Garfinkle/Hininger/Pferdehirt, first name row N, surname row N+1;
  this is the risky lookahead #2097 deferred) and CJK glued names
  (HechunWei/LushaNiu).

## Targeted unit fixtures (Codex ckpt-1 — regex is shared with #2088/#2094)

`_POSITION_ROLE_RE` / `_split_name_position` are read by the #2088 stacked-row
(`_position_only_cell`) and #2094 year-descending wrapped-title escape paths, so
`tests/test_sec_def14a_sct_parser.py` adds explicit cases, not just the panel:

- **Class A intra-cell wrap** (Alphabet): `'Sundar\n Pichai \n Chief Executive
  Officer…'` → name `'Sundar Pichai'`.
- **Mid-title face**: `'T. Wilson Eglin Chief Executive Officer\nand President'`
  → name `'T. Wilson Eglin'`.
- **Wrapped-name-only cell** (Cato): `'John\n P. D. Cato'` → name
  `'John P. D. Cato'` (whole-cell fallback, no role keyword).
- **Compound-title leak** (exec modifier): `'Raymond\nR. Quirk Executive Vice-
  Chairman'` → name `'Raymond R. Quirk'`.
- **#2088 stacked "Executive Chairman" title row** is classified position-only
  (does not open a bogus `'Executive'` NEO).
- **#2094 descending-year wrapped-title fragment with an embedded newline** still
  appends to the carried NEO (does not escape as a new NEO).
- **Surname-safety**: a name whose surname is a title-vocab word survives intact
  (`'Robert A. Bank'` stays `'Robert A. Bank'`) — proves no name mutation.
- **Non-lexicon-title regression guard**: `'James Dimon\nChairman and CEO'` →
  `'James Dimon'` unchanged.

## Backfill

Parser-version change → rewash all `def14a_body` (`scripts/rewash.py`) OR
`POST /jobs/sec_rebuild/run {"source":"sec_def14a"}`. Operator-visible figure:
`def14a_exec_compensation.executive_name` on the exec-comp panel — verify
Alphabet shows "Sundar Pichai", not "Sundar", post-backfill.
