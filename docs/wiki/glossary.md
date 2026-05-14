# Glossary

Plain-English definitions of domain terms used in eBull. Operator-
facing — for code-level definitions, see the relevant module
docstrings.

## Identifiers

- **CIK** — Central Index Key. SEC's 10-digit identifier for a
  filer (company or individual). Zero-padded. Example: Apple is
  `0000320193`.
- **CUSIP** — Committee on Uniform Security Identification
  Procedures. 9-character identifier for a US security (issuer +
  share class). Example: Apple common is `037833100`.
- **ISIN** — International Securities Identification Number. 12-char
  global identifier. US ISINs prefix with `US` and embed CUSIP.
- **LEI** — Legal Entity Identifier. 20-char global identifier for
  legal entities. Used in NPORT-P fund-series + holdings.
- **series_id** — SEC's identifier for a fund series within a
  registrant. Example: Vanguard 500 Index Fund is `S000002277`.

## Filings

- **10-K** — Annual report (US issuer).
- **10-Q** — Quarterly report.
- **8-K** — Material event ("current report").
- **13F-HR** — Quarterly institutional holdings report. Filed by
  every institutional manager with discretionary AUM > $100M.
  Filed within 45 days of quarter-end.
- **13D / 13G** — Beneficial ownership > 5%. 13D is activist intent;
  13G is passive. 13D filed within 10 days; 13G annually or on
  triggering events.
- **NPORT-P** — Mutual fund quarterly holdings snapshot. Public
  60-day-lagged version of monthly NPORT-MFP (which stays
  confidential).
- **N-CSR** — Mutual fund semi-annual + annual report. Audited at
  year-end (N-CSR); semi-annual (N-CSRS) is unaudited. NOT a v1
  holdings source: the OEF iXBRL has no per-holding identifier and
  the HTML SoI carries no CUSIP — see spike #918 / `docs/superpowers/spikes/2026-05-14-n-csr-feasibility.md`.
  eBull's manifest worker drains N-CSR rows via a synth no-op
  parser.
- **DEF 14A** — Proxy statement. Annual; carries director +
  officer compensation tables, treasury shares, beneficial ownership
  consolidated table.
- **Form 3 / 4 / 5** — Insider transaction reports.
  - Form 3: initial statement of beneficial ownership.
  - Form 4: changes in beneficial ownership (T+2).
  - Form 5: annual statement of changes not previously reported.

## FINRA

- **Bimonthly settlement date** — FINRA short interest is published
  twice a month (~14th + last business day of each month).
- **Days to cover** — `current_short / average_daily_volume`. Number
  of days of trading to close the entire short position at average
  volume. > 7 days indicates squeeze geometry.
- **RegSHO** — Regulation SHO. FINRA publishes daily short-sale
  volume per symbol.

## eBull-specific

- **Observation table** — Append-only event log. One row per
  filer-period-source observation. Tables named
  `ownership_<category>_observations`.
- **Current table** — "What's true now" snapshot. Refreshed by a
  writer that applies source priority + filed_at tie-break. Tables
  named `ownership_<category>_current`.
- **Rollup** — The aggregated "who owns what" view rendered on the
  operator card. See [`ownership-card.md`](ownership-card.md).
- **Tombstone** — A marker in `*_ingest_log` that a specific filing
  could not be ingested (parse error, fetch 404). Subsequent runs
  see the tombstone and skip without re-fetching.
- **Manifest worker** — The drainer in the jobs process that walks
  `sec_filing_manifest` rows in `pending` state, fetches the
  payload, parses, and writes to canonical tables. Rate-limited at
  10 req/s shared.
- **Re-wash** — Triggered re-parse of stored raw payloads after a
  parser-version bump. See `app/services/rewash_filings.py`.
- **Coverage banner** — UI indicator on the rollup card showing per-
  category freshness state: `fresh` / `stale` / `missing`.
- **Operator card / ownership card** — The frontend page where the
  rollup is displayed.
- **Kill switch** — DB-backed runtime flag that blocks all `BUY` /
  `ADD` execution. Separate from deployment config.

## Process

- **DoD** — Definition of Done. See `.claude/CLAUDE.md`. ETL /
  parser / schema-migration changes have additional clauses 8-12.
- **PREVENTION** — A class of review comment that points at a
  recurring repo-specific mistake. Resolved by adding to
  `docs/review-prevention-log.md` or a relevant skill file.
- **Codex** — The second-opinion AI used at three checkpoints:
  spec review, plan review, pre-push diff review. Distinct from the
  Claude review bot that runs on every PR push.
