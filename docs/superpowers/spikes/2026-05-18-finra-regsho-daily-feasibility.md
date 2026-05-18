# FINRA RegSHO daily short volume — feasibility spike (#916)

> Phase: 6 of `docs/superpowers/plans/2026-05-17-us-etl-completion.md` (PR 12).
> Issue: #916 (OPEN). Parent #845 CLOSED.
> Sibling: #915 PR #1207 merged 2026-05-18 (bimonthly short interest). Same `finra` Lane, shared throttle clock, separate cadence + schema.
> Verdict: **SHIP — sibling provider module + new daily ingest service + new partitioned observations table.**

## 1. Goal of the spike

Verify the empirical shape of the FINRA RegSHO daily CDN files BEFORE locking the spec for #916. Specifically:

1. Do all 6 prefixes (CNMS / FNQC / FNRA / FNSQ / FNYX / FORF) exist daily, or are some sparse?
2. What is the exact header + per-row format? Are volumes integer or decimal? Is `Market` single- or multi-valued?
3. What does a sparse / zero-row file look like (header-only? footer-only? both)?
4. Is the ~1 req/s polite budget still appropriate? Are the files small enough to make a per-day × 6 fan-out tractable?
5. Smoke: do the panel symbols (AAPL / GME / MSFT / JPM / HD) resolve directly?

## 2. Method

Empirically fetched the 2026-05-15 (Friday) prefix files via `curl` with the polite UA + 1 s sleep between fetches. Inspected header, body sample, footer, and CRLF behaviour with `head` / `tail` / `wc` / `file` / `od`.

Raw fetch script (reproducible):

```bash
for p in CNMS FNQC FNRA FNSQ FNYX FORF; do
  curl -sL \
    -A "eBull-dev/0.1 (luke.bradford@hotmail.co.uk)" \
    "https://cdn.finra.org/equity/regsho/daily/${p}shvol20260515.txt" \
    -o "${p}_20260515.txt"
  sleep 1
done
```

## 3. Findings

### 3.1 URL pattern + auth

```
https://cdn.finra.org/equity/regsho/daily/{PREFIX}shvol{YYYYMMDD}.txt
```

- Anonymous CDN; no key / OAuth / referer required.
- Same host (`cdn.finra.org`) as the bimonthly endpoint — shares the `finra` Lane introduced by #915. The module-global throttle clock + lock from `app/providers/implementations/finra_short_interest.py:46-48` MUST be reused in-process so the daily ingest does not double the FINRA budget when both jobs are active.
- HTTP 200 with `Content-Length` body on success; 404 when a date / prefix has no file yet.

### 3.2 Header + row shape

All six prefixes share the exact same 6-column header:

```
Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
```

Pipe-delimited. CRLF line terminators (verified via `file` — "ASCII text, with CRLF line terminators").

### 3.3 Volumes are DECIMAL (not integer) — KEY SCHEMA FINDING

Pre-spike assumption (per the parent plan brief) was integer-valued volumes. **Empirically refuted:**

```
20260515|AAPL|8714049.111124|41327|16275822.859084|B,Q,N
20260515|GME|1544093.403351|3647|2569999.110722|B,Q,N
20260515|A|329353.734525|0|571388.166174|B,Q,N
```

`ShortVolume` and `TotalVolume` are reported to 6 decimal places (per-symbol weighted aggregates across reporting facilities). `ShortExemptVolume` is integer-shaped in the panel sample but the column type is the same wire format and the schema MUST handle decimal regardless.

**Schema implication:** `NUMERIC(18, 6)` for all three volume columns. Gives 12 integer digits = 999 billion shares; 6 decimal places matches FINRA's stated precision.

### 3.4 `Market` column — single-char on facility files, multi-char on CNMS

Each non-CNMS prefix is single-facility:

| Prefix | Facility | Sample `Market` value |
|---|---|---|
| FNQC | FINRA/NASDAQ TRF Chicago | `B` (BX) |
| FNRA | ADF (legacy alt display facility) | `B` |
| FNSQ | FINRA/NASDAQ TRF Carteret | `Q` (NASDAQ) |
| FNYX | FINRA/NYSE TRF | `N` (NYSE) |
| FORF | ORF (OTC reporting) | `O` (OTC) |

CNMS aggregates across facilities and reports a comma-joined value:

```
20260515|AAPL|...|B,Q,N
20260515|GME|...|B,Q,N
```

**Schema implication:** `Market` must be `TEXT` (not `CHAR(1)` enum). The PK must include `market` to distinguish the CNMS row from the per-facility rows for the same instrument on the same date.

### 3.5 Sparse-prefix behaviour — FNRA is legitimately empty

`FNRAshvol20260515.txt` (the legacy ADF prefix) is 65 bytes total:

```
Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market
0
```

Just the header + a footer line `0` (the row-count footer — see §3.6). The ingest parser MUST treat zero-body files as a successful empty parse, not a defect. ADF is largely obsolete; the file persists daily for historical-coverage continuity.

### 3.6 Footer is a single-int row-count line

Every prefix file ends with a single line containing the integer count of body rows:

```
20260515|ZYBT|303|0|2006.046035|Q
20260515|ZYME|97204.061593|0|195319.379087|B,Q,N
11465
```

(For CNMS 2026-05-15: 11465 body rows + 1 header + 1 footer = 11467 lines.)

**Parser invariant:** the parser SHOULD assert that the parsed body row count matches the footer integer. Mismatch = file truncation in transit (rare CDN edge-case); raise a file-level defect + per-file failure.

### 3.7 File sizes

| Prefix | Bytes | Body rows |
|---|---|---|
| CNMS | 510,528 | 11,465 |
| FNQC | 171,965 | ~5,330 |
| FNRA | 65 | 0 |
| FNSQ | 484,416 | ~13,000 |
| FNYX | 251,876 | ~7,500 |
| FORF | 97,503 | ~2,800 |

Total ~1.5 MB / day × 252 trading days/year = ~380 MB/year. Per-file fetch on 1 req/s budget = 6 s per day. A 1-year backfill = 252 × 6 s ≈ 25 min wall-clock — tractable as a single REPL invocation for ETL DoD #10.

### 3.8 Panel-symbol smoke

All 5 panel symbols (AAPL / GME / MSFT / JPM / HD) resolve directly against the CNMS 2026-05-15 file:

```
20260515|AAPL|8714049.111124|41327|16275822.859084|B,Q,N
20260515|GME|1544093.403351|3647|2569999.110722|B,Q,N
20260515|HD|626038.450866|7637|2228061.122400|B,Q,N
20260515|JPM|1048430.286863|468|2145318.170192|B,Q,N
20260515|MSFT|7487114.713947|47840|18311938.181550|B,Q,N
```

No dotted-form-vs-FINRA-shape mismatch on the panel (none are share-class siblings / preferreds). The bimonthly `normalise_symbol` helper at `app/services/finra_short_interest_ingest.py::normalise_symbol` handles the dotted-form case for share-class instruments + preferreds — the daily ingester reuses it verbatim via re-import.

### 3.9 Date semantics

The `Date` column matches the URL date and is the **trade date** (not a settlement date). FINRA publishes the file the same evening as the trades (~6 PM ET); the file becomes available on `cdn.finra.org` shortly thereafter. The cron must fire AFTER ~6 PM ET on weekdays.

## 4. Decisions locked by this spike

| Decision | Implementation choice |
|---|---|
| Volume column type | `NUMERIC(18, 6)` for `short_volume`, `short_exempt_volume`, `total_volume`. NOT integer. |
| Market column | `TEXT` (max observed: comma-joined `B,Q,N` = 5 chars; allow up to 32 for safety). |
| PK | `(instrument_id, trade_date, market, source_document_id)`. `market` is part of PK because CNMS row + per-facility rows for the same `(instrument, trade_date)` are distinct facts. |
| `source_document_id` | `"{PREFIX}_{YYYYMMDD}"` (e.g. `CNMS_20260515`). Encodes the prefix so the audit trail distinguishes CNMS aggregate from facility-specific rows. |
| Accession | `"FINRA_REGSHO_{PREFIX}_{YYYYMMDD}"` — one manifest row per (date, prefix) file. 6 rows per day. |
| `subject_type` / `subject_id` | `finra_universe` / `FINRA_REGSHO` (single subject for the whole RegSHO daily slot). One freshness-index row tracks the daily ingest. |
| Provider | Sibling module `app/providers/implementations/finra_regsho.py` — clean separation from bimonthly; imports `_FINRA_RATE_LIMIT_CLOCK` + `_FINRA_RATE_LIMIT_LOCK` from the bimonthly module to share the throttle budget. Re-uses `FinraNotFound` from the bimonthly module. |
| Empty-file handling (FNRA) | Header parses fine; body has zero rows; footer is `0`. Service returns `SettlementIngestStats(rows_parsed=0, rows_upserted=0)` — NOT an error. Manifest row still written `parsed`. |
| Footer validation | After parsing body, assert parsed-row count == footer integer. Mismatch = `HeaderCorruptionError`-flavour file-level defect (rename to `FileFooterMismatchError` or fold into the existing `HeaderCorruptionError` family per Codex 1a feedback). |
| Cron cadence | Daily 23:00 UTC (≈ 7 PM ET, 1 h after FINRA's 6 PM ET publication window). Lane `finra` (already exists). Prerequisite `_bootstrap_complete`. |
| Revision window | Re-probe the 2 most-recent trade dates × all 6 prefixes regardless of manifest status. FINRA corrects RegSHO daily files in-place within 1-2 cycles, same pattern as bimonthly. |

## 5. Risks + open questions

| Risk | Mitigation |
|---|---|
| `Market` column drift (FINRA adds a new facility character) | `TEXT` column accepts arbitrary; PK includes `market` so a new facility character doesn't collide. Logs the new value at INFO; operator notices via the cross-source spot-check workflow. |
| Footer-validation false positive (CDN strips trailing newline) | Tolerant footer parse — strip whitespace, ignore trailing empty lines. Spike fixture verified the footer is on its own line. |
| Decimal precision overflow (volumes > 999 billion shares) | `NUMERIC(18, 6)` = 12 integer digits. Largest observed today: AAPL TotalVolume 16,275,822 — 8 digits. 12-digit headroom is ~10,000× current peak. If FINRA ever changes precision, the bot review on the migration will catch the overflow. |
| Same-day multi-fetch race (cron + manual trigger) | The `finra` lane's `JobLock` serialises both. No racing writes to the same (instrument, trade_date, market, source_document_id) row by construction. |
| Holiday gap | FINRA does NOT publish on US federal holidays. The 404 path returns `FinraNotFound`; the JOB skips silently per the bimonthly pattern. Next-fire is the next weekday; no operator action needed. |
| Forward-trade-date cron miss (cron fires before file is published) | `_compute_targets` always probes the last 2 trade dates; even if today's file hasn't published yet at 23:00 UTC, yesterday's catches up on the same fire. |

## 6. Verdict

**SHIP Option A-flavour** — sibling provider module reusing the bimonthly throttle state, dedicated service ingester, dedicated ScheduledJob, dedicated partitioned observations table (no `_current` snapshot). Schema + 4-call-site source-enum extension (sql/118 + sql/120 + `ManifestSource` Literal + `data_freshness.MAX_AGE_TARGETS`).

The spec proceeds against these locked decisions.
