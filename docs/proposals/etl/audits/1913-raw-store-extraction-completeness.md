# Raw-store extraction completeness audit (#1913)

Date: 2026-07-04. Author: autonomy loop. Status: **evidence only — retention/deletion is operator-gated** (ticket ask 4).

Operator direction (2026-07-04, re-raised post-crash): raw payloads should eventually be
removable — but only once we are confident every field we could possibly want is extracted
into structured tables. This doc produces that evidence: per raw store, what the payload
holds vs what we extract, the wanted-but-unextracted fields, and a verdict.

## TL;DR

- **The retention *legitimacy* question is already settled**, not open. Every raw filing
  payload is bucketed by the `#1617` three-class partition (`re-read` / `swept` /
  `kept-negligible`), CI-enforced by
  `tests/test_raw_payload_retention.py::test_every_document_kind_is_classified`. This audit
  does **not** re-open that; it answers the deeper *extraction-completeness* question the
  operator actually asked.
- **One store dominates and one verdict matters: `def14a_body`.** It is a *document_kind
  inside `filing_raw_documents`* (not a standalone table — see premise correction below),
  holds **29 GB uncompressed across 42,289 rows** (the single largest TOAST contributor to
  the 22 GB `filing_raw_documents` table, itself 44% of the 50 GB dev DB), and we extract
  **only SEC Item 403 (beneficial-ownership table)** from it. Exec-comp (Item 402),
  related-party (Item 404), governance/board, auditor fees, and shareholder proposals are
  **unextracted**. Verdict: **EXTRACT-MORE** — and it is the only store that reads that way.
- **Everything else is either intentionally KEEP-RAW (reparse substrate) or already
  COMPLETE.** No other store is a reclaim opportunity blocked on missing extraction.

## Premise corrections (full-population, dev DB `ebull`, 2026-07-04)

The ticket's "verified table names" list was stale. Corrected against live catalog:

| Ticket claimed | Reality |
|---|---|
| `def14a_body` (table, "largest retention lever") | **Not a table.** A `document_kind` value inside `filing_raw_documents`. Correct that it is the largest lever (29 GB uncompressed payload). |
| `financial_facts_raw` (table) | **Partitioned** table (`financial_facts_raw_YYYYqN`); parent `n_live_tup=0` misleads. **4,515,054 rows** across quarterly partitions (~7 GB summed). |
| `cik_raw_documents` (raw filing bodies) | Correct name; **0 rows in dev** (per-CIK `submissions.json`/`companyfacts.json` store, wired but populate-on-demand — see `app/services/cik_raw_filings.py`). |
| `filing_raw_documents`, `financial_periods_raw`, `raw_persistence_state` | Confirmed. Sizes: 22 GB / 142 MB / 64 kB. |

Dev DB total: **50 GB**. `filing_raw_documents`: **22 GB total** (859 MB heap + 21 GB
TOAST/index), **832,516 rows**.

## Source rule

- Raw-payload retention framework: `docs/settled-decisions.md` → "Raw-payload retention
  (#1617, settled 2026-06-13)". A stored payload is legitimate iff it is exactly one of:
  **re-read** (a `rewash_filings.registered_specs()` parser reads the body),
  **housekept-and-negligible** (born-compacted / swept — `raw_filings.SWEPT_DOCUMENT_KINDS`),
  or **kept-and-negligible** (small, write-only, justified — `raw_filings.KEPT_NEGLIGIBLE_DOCUMENT_KINDS`).
- Proxy-statement disclosure scope (what a DEF 14A *contains*, hence what is extractable):
  SEC Reg 14A / Schedule 14A Items — Item 402 (executive compensation), Item 403 (security
  ownership of certain beneficial owners and management), Item 404 (related-party
  transactions), Item 407 (corporate governance / board independence), plus auditor-fee and
  say-on-pay/proposal disclosures. Rule 13d-3 defines beneficial ownership (the Item 403
  denominator).

## `filing_raw_documents` — per document_kind (this is where the bytes are)

Full-population payload breakdown (`SUM(byte_count)`, `payload_swept_at` state):

| document_kind | rows | live | swept | payload (uncompressed) | #1617 class | extracted → structured target | verdict |
|---|--:|--:|--:|--:|---|---|---|
| **`def14a_body`** | 42,289 | 42,289 | 0 | **29 GB** | re-read | **Item 403 only** → `def14a_beneficial_holdings`, `ownership_def14a_observations`, `insider_transactions` (`app/services/def14a_ingest.py`) | **EXTRACT-MORE** |
| `infotable_13f` | 47,283 | 47,283 | 0 | 8.6 GB | re-read | 13F holdings → institutional ownership | KEEP-RAW (extraction complete for the info table) |
| `form4_xml` | 470,322 | 470,322 | 0 | 3.0 GB | re-read | insider txns → `insider_transactions` | KEEP-RAW |
| `primary_doc` | 144,909 | 38,412 | 106,497 | 2.7 GB | swept | n/a (re-fetch from EDGAR) | COMPLETE (housekept; 73% already born-compacted #1615) |
| `form3_xml` | 80,146 | 80,146 | 0 | 947 MB | re-read | initial ownership → insider tables | KEEP-RAW |
| `primary_doc_13dg` | 41,315 | 41,315 | 0 | 363 MB | re-read | 13D/G blockholders → `blockholder_filings` | KEEP-RAW |
| `nt_body` | 4,233 | 4,233 | 0 | 121 MB | kept-negligible | NT 12b-25 notice → `nt_filing_notices` | COMPLETE (kept-negligible, justified) |
| `finra_regsho_daily_txt` | 246 | 246 | 0 | 60 MB | kept-negligible | short volume (re-fetch) | COMPLETE |
| `finra_short_interest_csv` | 28 | 28 | 0 | 55 MB | kept-negligible | short interest (re-fetch) | COMPLETE |
| `form5_xml` | 1,588 | 1,588 | 0 | 11 MB | re-read | annual ownership → insider tables | KEEP-RAW |
| `nport_xml` | 152 | 152 | 0 | 10 MB | kept-negligible | fund holdings (reuse deferred #1731) | COMPLETE (kept-negligible) |
| `pre14a_body` | 5 | 5 | 0 | 4.6 MB | re-read (shares def14a parser) | Item 403 | EXTRACT-MORE (same as `def14a_body`, negligible size) |

### The `def14a_body` finding (the only material reclaim lever)

- **What raw holds:** the full DEF 14A / DEFA14A / DEFM14A proxy body (HTML), ~700 KB avg.
- **What we extract:** `app/services/def14a_ingest.py` runs `parse_beneficial_ownership_table`
  and writes **only the Item 403 beneficial-ownership table**. The rewash spec
  (`rewash_filings.py::_apply_def14a`, `current_version="def14a-v1"`) re-reads the stored
  body on re-drain and rewrites those same holder rows. Because it is a **re-read** kind, the
  payload can never be swept while the parser exists — this is *by design*, and it is why
  29 GB persists.
- **Wanted-but-unextracted (thesis/scoring relevant):**
  - **Item 402 — executive compensation** (CEO/NEO pay, pay-vs-performance, option grants).
    Direct thesis-engine input (governance/alignment signal); currently zero extraction.
  - **Item 404 — related-party transactions** (insider self-dealing red flags).
  - **Item 407 — board composition / independence / committee membership** (governance
    quality signal).
  - **Auditor identity + fees** (audit-quality / going-concern proximity signal).
  - **Shareholder proposals + say-on-pay vote outcomes** (activism / dissent signal).
- **Verdict: EXTRACT-MORE, then KEEP-RAW until the extraction ceiling is operator-declared.**
  Two mutually-exclusive paths to reclaim the 29 GB, both operator-gated:
  1. **Extract the remaining thesis-relevant items** (Item 402 comp at minimum), *then* the
     raw is only deletable once every wanted field is declared captured and the rewash parser
     is retired (moving the kind from `re-read` to `swept`, re-fetchable from EDGAR by
     accession — proxies are immutable per accession, so re-fetch is lossless).
  2. **Operator declares Item 403 the sole wanted field** and accepts re-fetch-on-reparse:
     retire `_apply_def14a` from `registered_specs()`, add `def14a_body` to
     `SWEPT_DOCUMENT_KINDS`, born-compact per #1615. Reclaims ~29 GB uncompressed
     (dominant share of the 21 GB TOAST). **This is the single highest-value retention
     decision available and it is a genuine operator judgment call** (is future proxy
     re-mining worth 29 GB, vs. re-fetch latency at 10 req/s).

Everything else in the table is either already housekept (`primary_doc`) or a deliberate
KEEP-RAW reparse substrate whose extraction is complete for its structured target — no
reclaim is blocked on *us* missing a field.

## `financial_facts_raw` (partitioned XBRL fact store)

- **Holds:** 4,515,054 raw XBRL facts (`taxonomy, concept, unit, period_start/end, val,
  frame, decimals, …`), partitioned by quarter. Dominated by 10-Q (2.88 M) / 10-K (1.06 M) /
  20-F (258 k).
- **Extracted:** read by `app/providers/implementations/sec_fundamentals.py` +
  `app/providers/fundamentals.py` into the derived period layer (`financial_periods_raw` →
  `financial_periods`) and consumed by risk/valuation. It is the **reparse substrate** for
  every fundamentals figure.
- **Wanted-but-unextracted:** none structurally — it is the fact-level source of truth;
  derivation (not extraction) is where gaps live (e.g. #1914 FY-history keying, #1823
  restated-comparative ordering). Those are *derivation* bugs, not raw-retention gaps.
- **Verdict: KEEP-RAW.** Reparse substrate for all fundamentals; cannot be dropped without
  losing the ability to re-derive periods under a new taxonomy mapping. (Its **index
  footprint** — a separate concern — is tracked by #1620, not this audit.)

## `financial_periods_raw` (derived per-period staging)

- **Holds:** 217,917 rows, ~60 typed financial columns (revenue…public_float_usd) —
  this is **already the structured layer**, "raw" is a staging misnomer (pre-`financial_periods`
  reconciliation). 142 MB.
- **Verdict: COMPLETE (not a payload store).** No opaque payload to mine; it is typed output.
  Retention is a normalization/dedup concern (#541 migrate readers off `fundamentals_snapshot`),
  not an extraction-completeness one.

## `cik_raw_documents` (per-CIK submissions/companyfacts store)

- **Holds:** per-CIK `submissions.json` + `companyfacts.json` bodies (rolling, keyed by CIK,
  not accession — deliberately separated from `filing_raw_documents` per PR-808 review).
  **0 rows in dev** (populate-on-demand; `store_cik_raw` is idempotent overwrite).
- **Verdict: COMPLETE (write-through, re-fetchable).** Bodies are rolling snapshots
  re-fetchable from SEC at any time; no historical value in the stored copy beyond a request
  cache. No unextracted wanted field. If dev bloat ever appears here it is safe to truncate.

## `raw_persistence_state` (sweep bookkeeping)

- **Holds:** per-source compaction/sweep watermarks (`last_compacted_at`,
  `last_compaction_bytes_reclaimed`, …). **0 rows dev**, 64 kB. Not a payload table.
- **Verdict: N/A — operational, keep.** No payload; drives the #1014/#1615 sweep. Out of
  scope for retention reclaim.

## Consolidated verdict table

| Store | Size | Verdict | Reclaim lever? |
|---|--:|---|---|
| `filing_raw_documents` / `def14a_body` kind | 29 GB payload | **EXTRACT-MORE** | **Yes — the only material one (operator-gated, ~29 GB)** |
| `filing_raw_documents` / other re-read kinds | ~12 GB payload | KEEP-RAW | No (reparse substrate, extraction complete) |
| `filing_raw_documents` / `primary_doc` | 2.7 GB (73% swept) | COMPLETE | Already housekept |
| `filing_raw_documents` / kept-negligible kinds | ~250 MB | COMPLETE | No (justified small) |
| `financial_facts_raw` (partitioned) | ~7 GB | KEEP-RAW | No (fundamentals substrate; index size → #1620) |
| `financial_periods_raw` | 142 MB | COMPLETE | No (typed staging, not a payload) |
| `cik_raw_documents` | 0 rows dev | COMPLETE | Truncate-safe (re-fetch cache) |
| `raw_persistence_state` | 64 kB | N/A operational | No |

## Recommended operator follow-up (NOT actioned here — evidence only)

1. **Decide the `def14a_body` question** (the whole game): either fund Item 402/404/407
   extraction (new parser scope, thesis-engine value) **or** declare Item 403 the sole wanted
   field and sweep the kind to reclaim ~29 GB. File the winner as a scoped ticket; both are
   real work, not in-loop-autonomous (parser scope = product decision; sweep = irreversible
   raw loss).
2. No other store warrants a retention change on completeness grounds.

## Verification (full-population, not sampled)

- Sizes/rows/payload-by-kind: live queries against dev `ebull` on 2026-07-04 (catalog +
  `SUM(byte_count)` over the full `filing_raw_documents` population; XBRL fact count over all
  `financial_facts_raw_*` partitions).
- Extraction targets: read `app/services/def14a_ingest.py`, `app/services/rewash_filings.py`
  (`registered_specs()`, `_apply_def14a`), `app/services/raw_filings.py`
  (`DocumentKind` / `SWEPT_DOCUMENT_KINDS` / `KEPT_NEGLIGIBLE_DOCUMENT_KINDS`),
  `app/services/cik_raw_filings.py`, and the `financial_facts_raw` reader providers.
- Framework: `docs/settled-decisions.md` "Raw-payload retention (#1617)";
  `tests/test_raw_payload_retention.py`.

Related: #1620 (`financial_facts_raw` index footprint — size angle, not completeness),
#1914 (fundamentals FY-history derivation — a derivation gap, not a raw-retention gap),
#541 (`fundamentals_snapshot` → `financial_periods` reader migration).
