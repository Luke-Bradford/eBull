# ETF trust CIKs — design decision (#1577)

**Status:** draft for Codex ckpt-1 + operator sign-off.
**Scope:** decide where ETF trust CIKs live in the identity model. No code in this doc's PR; implementation tickets follow the decision.

## Problem

~457 US-listed tradable ETFs carry no `(sec, cik)` row. Their tickers live in SEC's `company_tickers_mf.json`, keyed to the **trust** CIK: iShares Trust `0001100663` alone covers ~300 of our ETF instruments. Stamping trust CIKs through the #1102 shared-CIK mechanism would:

1. Blow the settled-decision threshold — the per-instrument shared-CIK indexes were sized for ~10 share-class siblings with an explicit "50+ → entities layer" tripwire. Dev is at **46 multi-bound CIKs** today; one trust adds ~300.
2. Explode parse-time fan-out — CIK→instrument fan-out consumers (issuer manifest parsers via `_siblings.py::resolve_siblings`, the three bulk ingesters' multimaps) fan per-filing writes across all siblings of the filing's CIK. A trust-level filing (N-CEN, 485BPOS) would denormalise ×300. (N-CSR is NOT in this class — it fans out by class_id, deliberately.)
3. Mis-route subject resolution — `sec_atom_fast_lane.py::default_subject_resolver` resolves CIK→instrument with `LIMIT 1`; trust filings would land on an arbitrary sibling ETF.

## Evidence (dev DB, 2026-06-11)

- 1,869 us_equity tradable instruments lack a CIK; **440 of them hold a `(sec, class_id)` row AND appear in `cik_refresh_mf_directory`** — the ETF series/class population (close to the 457 estimate; remainder = delisted/operational variants).
- `cik_refresh_mf_directory` (sql/149) already stores **`trust_cik` per class row** — 28,379 classes across 1,169 trusts; biggest trusts 339-587 classes each. Trust CIK is queryable today without touching instrument identity. Caveat: the directory has observed-ever semantics (no tombstoning on SEC dropping a class); any future trust-CIK consumer JOINing through it needs a freshness predicate (`last_seen`).
- Fund data already flows without instrument-level trust CIKs, via two distinct paths: N-PORT holdings keyed by `fund_series_id` (#1171); N-CSR/fund-metadata resolved per class via `_fund_class_resolver.resolve_class_id_to_instrument` — N-CSR **already walks trust CIKs** from the directory and lands per-instrument fund metadata through class_id fan-out.
- Berkshire counter-lesson (PR #1579): CIK 0001067983 was unbound until 2026-06-11; its historical manifest rows resolved `subject_type='institutional_filer', instrument_id=NULL` and stay mis-subjected — **no re-resolution sweep exists for late-arriving bindings**. Any option must not make late trust-CIK arrival worse.

## What instrument-level data would a trust CIK even deliver?

| Data class | ETF source | Needs trust CIK on instrument? |
| --- | --- | --- |
| Holdings | N-PORT keyed by `fund_series_id` | No (wired, #1171) |
| Fund metadata (expense, objective) | N-CSR via trust-CIK walk → class_id resolution | No (wired — consumes the directory, not instrument CIK rows) |
| Fundamentals (10-K/Q facts) | ETFs file none | No |
| Insider (Form 3/4/5) | None for ETFs | No |
| Proxy (DEF 14A) | Trust-level, fund-board ops | Marginal |
| N-CEN / 485 (fees, ops) | Trust-level | Only consumer that would |
| Prices / AUM | Market data providers | No |

No current consumer needs trust CIK **stamped on instruments** — N-CSR proves trust-CIK-keyed ingest works fine through the directory + class-resolution path. No eBull surface consumes N-CEN/485 content today.

## Options

### (a) Entities layer (#1102 Option B)

`entities(cik PK, kind, name)` + `instruments.entity_id FK`; entity-level data keyed by entity; readers join. Honest model, kills denormalisation, survives 300-sibling trusts.
**Cost:** large migration touching every CIK-keyed reader (fundamentals, submissions, insider, capabilities, manifest subject resolution) + the two-layer ownership model. Weeks, not days. Pays rent only when a consumer needs trust-level data.

### (b) Non-primary trust-CIK rows + audited consumer contract

Stamp `(sec, cik, trust_cik_value, is_primary=FALSE)` on ETF instruments; audit every CIK→instrument consumer to filter `is_primary` or cap fan-out.
**Cost:** the audit is the hard part — every future consumer must remember the filter; one miss re-creates the ×300 explosion silently. The partial unique indexes (sql/143) also key on `(provider, type, value, instrument_id)` regardless of primacy, so 46→346 multi-bound CIKs immediately, deep in tripwire territory. Fragile.

### (c) By-design: series/class is the ETF identity path — RECOMMENDED

Trust CIK is **deliberately not stamped** on instruments. ETF fund-side data flows via `class_id`/`mf_directory` (which already carries `trust_cik` for any future consumer to JOIN through). The instrument-level CIK gap for ETFs is by-design, not a coverage failure.

Implementation (one small PR):
1. `docs/settled-decisions.md` entry: "ETF identity = series/class; trust CIK lives in `cik_refresh_mf_directory`, never on `external_identifiers`."
2. `cik_coverage_audit`: new bucket `fund_series_covered` — unmapped instrument with a **primary** `(sec, class_id)` row whose class_id still exists in `cik_refresh_mf_directory` (the directory join guards against stale/demoted class_ids hiding a real CIK gap). The 440 ETFs stop polluting the actionable `other` bucket and the audit stays honest.
3. Revisit-tripwire in the settled-decision, narrowly drawn: file option (a) when a trust-CIK-keyed consumer **cannot resolve to series/class before instrument writes** (the N-CSR pattern stops fitting), or when a durable entity-level trust page/join is needed. Trust-level content per se is NOT the tripwire — N-CSR already renders trust-filed data per-ETF through class resolution. No half-step through (b).

### Out of scope (follow-ups filed separately)

- Manifest subject re-resolution sweep for late-arriving CIK bindings (Berkshire case, PR #1579 evidence).
- The handful of operating companies absent from all three SEC ticker files (AL, DHIL, FBMS, VRNA, CVAC) — manual-override endpoint, re-homed from #813.

## Decision rationale

Trust CIK is already captured as fund-directory/filer identity; instrument identity for funds is class_id/series_id. Stamping trust CIK into issuer-style `external_identifiers (sec, cik)` rows would violate the current fan-out and subject-resolution invariants — that, not cost alone, is the core argument. (c) keeps the #1102 mechanism honest at its designed scale (~10 share-class siblings), keeps the entities-layer powder dry behind a narrow tripwire, and nothing is thrown away by deferring: the data a future consumer would want is already captured (`trust_cik` per class, sql/149).
