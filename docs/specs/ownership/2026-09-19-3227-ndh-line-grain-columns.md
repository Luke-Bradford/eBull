# #3227 item 2 — carry the Table I line-grain discriminators on `:NDH:` rows

Status: proposed, 2026-09-19. Scope: acceptance item 2 of #3227 only. Items 3-5 (the
`_current` re-key, the joint-filing refusal, the A/B on the repaired key) stay open.

This pass adds **evidence columns**. It changes no key, no `_current` row and no
operator-visible figure.

## Source rule

Form 3 General Instruction **5(b)(iii)**: *"Report securities beneficially owned
directly on a separate line from those beneficially owned indirectly. **Report different
forms of indirect ownership on separate lines.** The nature of indirect ownership shall
be stated as specifically as possible."* Form 4 General Instruction **4(b)(iii)** carries
the same separation requirement, with the subject widened — *"Report [transactions in]
securities beneficially owned directly on a separate line…"*. ⚠ The two are **not
verbatim identical**; only the separation sentence is. Both sit under Table I's printed
reminder *"Report on a separate line for each class of securities beneficially owned
directly or indirectly."* ⚠ `:NDH:` also carries **Form 5** holdings, governed by Form
5 General Instruction 4(b)(iii) to the same effect.

Line grain is therefore **(class of security) x (direct | each distinct form of
indirect)**. Skill reference: `.claude/skills/data-sources/sec-edgar.md` §2.3, the
`:NDH:` aggregation block — which also records that `ownership_nature` on a `:NDH:` row
is the DERA *relationship* flag, not the Table I D/I field, so neither axis is in
`ownership_insiders_current`'s key today.

The holding key itself is the SEC's, not ours: the DERA Insider Transactions readme
documents `NONDERIV_HOLDING_SK` as the holding surrogate key under `ACCESSION_NUMBER`.
The census below confirms our corpus conforms to it; conformity is evidence, the readme
is the rule.

## Full-population verification (dev, 2026-09-19, read-only)

Reproduce with:

```bash
PYTHONPATH=. uv run python -m scripts.census_3227_insider_holding_line_collapse --source-columns
```

⚠ **"Full population" here means every archive this deployment has cached — 81 quarters,
2006q1..2026q1, 2,367,536 `NONDERIV_HOLDING` rows.** SEC publishes later quarters; header
stability observed is not a guarantee of future stability.

### Header stability

**One** distinct header signature across 81/81 archives; zero archives missing any
wanted column.

### Fill rates and key integrity — the whole cached corpus

| measure | count | rate |
|---|---:|---:|
| rows | 2,367,536 | 100.00% |
| `SECURITY_TITLE` non-empty | 2,367,536 | 100.00% |
| `DIRECT_INDIRECT_OWNERSHIP` non-empty | 2,367,536 | 100.00% |
| `NATURE_OF_OWNERSHIP` non-empty | 1,720,305 | 72.66% |
| `NONDERIV_HOLDING_SK` blank | 0 | 0.00% |
| duplicate `(ACCESSION_NUMBER, NONDERIV_HOLDING_SK)` within an archive | 0 | 0.00% |
| `{accn}:NDH:{sk}` ids appearing in more than one archive | 0 | 0.00% |

`DIRECT_INDIRECT_OWNERSHIP` takes exactly two values: `I` 1,720,305 / `D` 647,231.
`SECURITY_TITLE` has 9,963 distinct values. ⚠ A scratch scan during research said 9,422;
that was an artefact of truncating titles to 40 characters in the throwaway script. The
census is authoritative and this figure comes from it — which is the point of making the
census the derivation rather than quoting a number. Stored side: **0** `:NDH:0` observation rows
exist, so the blank-SK fallback the census registered as a risk has a population of zero
on both sides.

### ⚠⚠ What the backfill can actually reach — Codex ckpt-1 finding, confirmed

The holdings loop begins `if accn in accessions_with_transactions: continue`
(`sec_insider_dataset_ingest.py:734`). **Measured: 1,840,672 of 2,367,536 holding rows
(77.75%) sit on an accession that also has `NONDERIV_TRANS` rows and are therefore never
written at all.** Only **526,864 (22.25%)** are reachable by this writer.

An earlier draft of this spec claimed the observations layer "already holds the lines
that `_current` discards". **That is true only of the reachable 22.25%** and is corrected
here. Stored today: 312,849 `:NDH:` observation rows over **262,833** distinct
`source_document_id`s (ratio 1.19 = the owner x `#1117` share-class fan-out). The
backfill's denominator is those 262,833 ids, not the corpus.

This bounds item 2, and it does not invalidate #3227: the census's 3,087 affected
`_current` rows are inside the reachable set. The 77.75% is a **separate, larger defect**
in the same writer and is recorded on the issue rather than fixed here.

### Trust levels differ per column — they are NOT interchangeable

Cross-checked against an independent parse of the same filings
(`insider_initial_holdings`, written by `insider_transactions._parse_one_holding`, which
reads `securityTitle`, `postTransactionAmounts`, `directOrIndirectOwnership` and
`natureOfOwnership` **from the same `<nonDerivativeHolding>` element**, so it cannot
permute fields across lines). Joined on `(accession, security_title, shares)` over
non-derivative rows: 50,313 matched / 1,398 ambiguous / 2,085 unmatched.

| column | agreement on the matched subset |
|---|---|
| `DIRECT_INDIRECT_OWNERSHIP` | **50,313 / 50,313 — zero disagreements** |
| `NATURE_OF_OWNERSHIP` | 40,984 agree, **9,329 differ (18.5%)** |

⚠ **This is a SUBSET check, not a full-population one.** 50,313 rows is ~2.1% of the
cached corpus — it is bounded by the accessions for which this deployment holds a parsed
Form 3 XML (42,767). The 1,398 ambiguous and 2,085 unmatched rows carry no assurance.

⚠ **`SECURITY_TITLE` is NOT verified correct by this check — it is part of the join
predicate, so the test is circular for that column.** Title is verified *populated*
(100%) and nothing more. A title mismatch would silently land in the unmatched 2,085.

The 9,329 nature differences break down as: `xml_blank_dera_has` 4,099,
`dera_blank_xml_has` 3,744, **`substantive` 1,484**, case-only 1, punctuation-only 1.
⚠ The blank/non-blank pair (7,843) **dominates and is unexplained** — it is not
accounted for by the permutation reading below.

On the substantive 1,484, two worked accessions show DERA and our parse holding the
**same value set attached to different lines** — `0000014693-16-000152`
(`Trust FBO Child #1/#2`, `Trust FBO Geo Garvin Brown IV`, `CBGB LLC`) and
`0001144204-16-120758` (`By Spouse` / `By LLC` / `By Partnership`). ⚠ **Two accessions
are not a measurement of 1,484**; the permutation reading is a hypothesis consistent with
the examples inspected, not an established characterisation of the whole 1,484, and set
equality would in any case discard multiplicity. Ground truth from EDGAR could not settle
it: `settings.sec_user_agent` is the placeholder `eBull dev@example.com` and the document
endpoints soft-404 under it while the directory listing succeeds.

**Consequence, and it binds item 3.** What the evidence supports is a *quarantine*, not a
verdict: `nature_of_ownership` has a measured 18.5% disagreement with an independent
parse of the same filings and **must not be keyed on** until that is explained.
`direct_indirect` is the one column with positive cross-source confirmation.
`security_title` is populated but unverified. The key Instruction 5(b)(iii) supports on
cross-confirmed columns alone is `(class) x (direct|indirect)` — the instruction minus
its "each form of indirect" refinement, which knowingly merges distinct indirect forms
and is item 3's trade-off to make explicitly.

### A tie that was algebra, not evidence

The cross-tab reads `(D, nature)` 150,452 and `(I, no-nature)` 150,452 — equal, and equal
again inside every one of 142,671 accessions (0 exceptions). That looked like proof of a
D/I transposition. It is not: per-accession `#nature-non-empty` equals `#I`, and given
that identity `#(D,nature) == #(I,no-nature)` follows **by construction**. D/I is
independently confirmed correct 50,313/50,313. Recorded because the exact tie is
persuasive and wrong.

### `DISTINCT ON` winner stability — Codex ckpt-1 finding, population zero

`_INSERT_FROM_STG_SQL` breaks ties with `ctid DESC`, which is physical placement, not
insertion order; widening staging rows changes placement. For a `:NDH:` row to be
affected, two staging rows must collide on
`(instrument_id, identity, ownership_nature, source, source_document_id, period_end)`.
Doc ids are unique per line (above), so a collision requires the **same reporting-owner
identity twice on one accession**. Measured over `REPORTINGOWNER.tsv` for all 81
archives: **4,402,307 accessions, 0 with a duplicate owner identity, 0 excess rows.**
No `:NDH:` tie exists for `ctid` to adjudicate, so the widening cannot move a winner.
The concern is real in general and empty here.

## Change

1. **Schema** — `sql/400_insider_observation_line_grain.sql` adds three nullable columns
   to `ownership_insiders_observations` (partitioned, 125 partitions): `security_title
   TEXT`, `direct_indirect TEXT` with `CHECK (direct_indirect IS NULL OR direct_indirect
   IN ('D','I'))`, `nature_of_ownership TEXT`. No index, no key change, no `_current`
   change.

2. **DERA writer** (`sec_insider_dataset_ingest`) — the `:NDH:` loop reads the three TSV
   columns; `_STG_COPY_COLUMNS`, `_CREATE_STG_SQL`, `_stage_owners`'s signature and its
   positional `copy.write_row` tuple, and `_INSERT_FROM_STG_SQL` (both the insert list
   and the `DO UPDATE SET`) all carry them. The `DISTINCT ON` key and `ORDER BY` are
   **frozen** — the new columns are payload only.

   **Parsing policy, identical in ingest and backfill:** strip whitespace; empty becomes
   `NULL`; `direct_indirect` is sanitised to `NULL` unless it is exactly `D` or `I`,
   mirroring the existing XML sanitiser at `insider_transactions.py:1241`. ⚠ The
   sanitiser is load-bearing, not defensive: `COPY … ON_ERROR ignore` protects the COPY
   into staging but **not** the `INSERT … SELECT` into the target, so an unexpected D/I
   value reaching the target `CHECK` would abort the whole drain.

   **Update semantics:** the `DO UPDATE SET` assigns `EXCLUDED` unconditionally, matching
   every other column in that clause. A re-ingest of the same archive is therefore
   idempotent, and a source that legitimately clears a value can clear ours. It also
   means an `:NDT:` re-write of a row would blank these — which cannot happen, because
   doc ids carry the `:NDT:`/`:NDH:` marker and so never collide across the two loops.

3. **`:NDT:` is deliberately NOT carried.** `NONDERIV_TRANS.tsv` publishes the same three
   columns (header verified). Item 2's acceptance names `:NDH:` rows, and `:NDT:` is
   #3146's ordering surface — a different fix. Recorded so the next session knows the
   columns are available rather than absent.

4. **XML writer** (`ownership_observations.record_insider_observation`) — gains the three
   as optional keyword arguments defaulting to `None`, so existing callers are unchanged.
   Its callers emit **collapsed groups** (one row per `(filer_cik, direct_indirect)`;
   `ownership_observations_sync` additionally groups transactions by filer name and
   iterates holdings with no `ORDER BY`, so its conflict winner is undefined). They pass
   nothing in this pass.

   ⚠ Codex is right that this is a **scope decision, not an impossibility** — those
   writers do retain a selected line whose attributes are knowable. Populating them
   belongs with item 3, which is where the collapse is actually repaired.

   ⚠ Codex is also right that **`NULL` here is overloaded** and cannot be read as
   "collapsed group": it equally means an `:NDT:` row, a pre-backfill row, an absent
   source value, or an unreachable holding. Any consumer needing the distinction must get
   explicit grain/provenance metadata — not infer it from `NULL`. Stated so item 3 does
   not inherit a false affordance.

5. **Backfill** — `scripts/backfill_3227_ndh_line_grain.py` stages archive-derived
   `(source_document_id, title, dio, nature)` into a temp table and runs a single
   `UPDATE … FROM`. Matching on `source_document_id` is safe for the owner x instrument
   fan-out precisely *because* every replica shares one source line and one payload
   (multiplicity across archives measured at 0), so updating all replicas is correct
   rather than ambiguous. There is no index on `source_document_id` across the 125
   partitions, hence one staged join rather than per-row updates.

   It writes **only** the three new columns — no row is created or deleted, no §16 gate
   is re-adjudicated, no `shares` value moves. That is why it is preferred to a full
   `sec_rebuild` of the insider sources.

## Acceptance

Aggregate invariance is not sufficient — compensating changes can hide corruption — so:

- Migration applies; the three columns exist on parent and all 125 partitions.
- **Exact keyed comparison**, not counts: every pre-existing `ownership_insiders_
  observations` field and every `ownership_insiders_current` row is byte-identical
  before and after, compared on the full key. A row-count or sum match alone does not
  pass this clause.
- **Source-value equality**: for every updated row, the three new columns equal the
  archive payload for its `source_document_id`.
- **Fan-out reconciliation**: report distinct source lines matched, target rows updated,
  and target `:NDH:` rows left unmatched, with the unmatched population explained.
  Denominator is the 262,833 stored ids, not the 2.37M corpus.
- Both writers exercised, not only the backfill: a re-ingest of one archive must produce
  the same values the backfill wrote.
- DoD clauses 8-12: panel `AAPL`, `GME`, `MSFT`, `JPM`, `HD` plus `MNSO` and `CNH` (the
  ticket's worked examples). ⚠ A seven-symbol panel is a **sample** and establishes
  only that the endpoint renders; the invariance claim rests on the keyed comparison
  above, which is full-population.

## Deliberately out of scope, recorded so it is not lost

- The 77.75% holdings-loop skip (above) — larger than this ticket, same writer.
- `*_FN` footnote columns and the DERA `FOOTNOTES` table, which can qualify both class
  and ownership distinctions. Not carried; item 3 must not assume the three columns are
  sufficient without checking them.
- Which source mis-pairs `NATURE_OF_OWNERSHIP` — unresolved, blocked on a real
  `sec_user_agent`.

## Security

No security surface — SEC reference data, no authorisation boundary, no user input.
