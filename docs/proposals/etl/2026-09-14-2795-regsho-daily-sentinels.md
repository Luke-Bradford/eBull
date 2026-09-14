# #2795 — RegSHO daily's match-rate sentinel, and the retention arm that does NOT transfer

Status: spec, **v2**. Refs #2795, #2337, #2234, #1955.

v1 proposed lifting all three of #2337's arms and running them per tape, with a retention
floor "constructed from the failure it must catch". Codex checkpoint 1 returned 32 findings
and the construction did not survive; §3 records why, because that is the load-bearing part
of this ticket. v2 ships arms 1 and 2 and **does not ship a retention arm for RegSHO**.

## 1. The defect

`app/jobs/finra_regsho_daily_refresh.py:322-332` carries a byte-equivalent copy of the arm
#2337 removed from the bimonthly sibling: WARN when `total_resolved / total_parsed < 0.50`,
pooled across all tapes.

Root cause is #2337's, recorded at `docs/review-prevention-log.md:5067`: the ratio's two
sides are governed by **different populations** — the numerator is bounded by OUR universe
(`build_preloaded_symbol_resolver` selects `instruments WHERE is_tradable`), the denominator
by FINRA's. It has no healthy value anyone could have written down.

RegSHO adds a second fault: it pools tapes whose structural match rates differ by more than
an order of magnitude, so the pooled ratio straddles the floor and the alarm fires on ~38%
of days with no relationship to anything being wrong. An alarm that is silent most of the
time *looks* discriminating, which is worse than #2337's always-on one.

⚠ **Three corrections to the ticket's own text:**

- It says "six tapes". Six are fetched and stored (540 payloads, 90 dates each); five carry
  rows. The sixth, `FNRA`, is the legacy **ADF** facility documented as *"often empty"*
  (`app/providers/implementations/finra_regsho.py:52`), and §6's replay measures its body
  as empty on **90 of 90** dates rather than inferring it. An empty body is a SUCCESS path
  (`finra_regsho_ingest.py:29`) — ⚠ for **every** prefix, not just this one.
- It calls `FORF` "the OTC/ADF tape". `FORF` is the **ORF** (OTC Reporting Facility);
  `FNRA` is the ADF (`finra_regsho.py:52,55`). Two different facilities.
- ⚠⚠ **The five are not disjoint populations.** `CNMS` is the *consolidated* NMS file and
  overlaps the per-facility tapes by construction (`finra_regsho.py:49`). Any claim resting
  on "N independent pairs" across all five is overstated, and v1 made one.

## 2. Measurement, and what it is a measurement OF

Per-tape consecutive-trade-date **stored-observation** counts, 90 dates per tape
(2026-05-05 → 2026-09-11), 89 pairs each:

| tape | facility | rows/day | min ratio | p05 | pairs < 0.95 | pairs < 0.90 |
| --- | --- | ---: | ---: | ---: | ---: | ---: |
| CNMS | consolidated (overlaps below) | 5,455–5,559 | 0.9956 | 0.9970 | 0 | 0 |
| FNSQ | NASDAQ TRF Carteret | 5,452–5,553 | 0.9951 | 0.9967 | 0 | 0 |
| FNYX | NYSE TRF | 4,872–5,033 | 0.9769 | 0.9878 | 0 | 0 |
| FNQC | NASDAQ TRF Chicago | 3,693–4,250 | 0.9139 | 0.9399 | **7** | 0 |
| FORF | OTC Reporting Facility | 54–75 | 0.8611 | 0.9057 | **17** | **3** |

⚠⚠ **This is NOT a measurement of `rows_resolved`, and the difference is why it cannot
calibrate a retention arm even in principle.** `rows_resolved` counts resolved BODY rows
(`finra_regsho_ingest.py:212`); the table's PK is
`(instrument_id, trade_date, market, source_document_id)`, so two body rows for the same
instrument and market collapse to one stored row, and the `ON CONFLICT … DO UPDATE` has no
DELETE, so a re-ingest that resolves **fewer** rows leaves the surplus in place. Stored count
is therefore a high-water mark, not the counter the arm would compare. Reproducing the real
series needs a parser replay against `filing_raw_documents.payload`, not a `count(*)`.

The table is kept because it is sufficient for the **negative** conclusion in §3 — it
establishes that the tapes' day-to-day variation differs by an order of magnitude, which no
re-measurement would reverse. It is not sufficient to calibrate anything, and v1 used it as
if it were.

## 3. The finding — no retention floor is constructible for RegSHO, so none ships

#2337's `_RESOLVED_RETENTION_FLOOR = 0.90` is correct for the bimonthly corpus (worst
consecutive drop 0.07%). It does **not** transfer. A 0.90 floor fires on FORF three times in
90 days for nothing; 0.95 fires 24 times. Three candidate constructions were tried and all
three failed:

1. **A fitted per-tape floor.** That is the miscalibration this ticket exists to fix, one
   level down. `docs/review-prevention-log.md:5071` already records that re-baselining "buys
   a few years and then repeats".
2. **A `sqrt(n)` counting-noise band**, `1 - k/sqrt(n_prev)`, which would be knowable by
   construction if daily counts moved as counting noise. The `k` each tape's worst pair
   requires: CNMS 0.32, FNSQ 0.36, FORF 1.18, FNYX 1.64, **FNQC 5.60**. ⚠ v1 called that
   "5.6 sigma"; **that arithmetic was wrong** — for a difference of two independent Poisson
   counts the variance is ~`2n`, so it is ≈3.96σ. The conclusion is unchanged and the
   correction matters: FNQC's day-to-day variation is not counting noise, so the model does
   not hold for it, and no single `k` is both silent on FNQC and tight enough elsewhere to
   mean anything.
3. **A "half the previous day" floor**, on the argument that a resolver break removes a large
   fraction rather than shaving a few percent. ⚠ **This is a dressed-up pick and is
   withdrawn.** It has no independently specified detection requirement — nothing says what
   loss must be caught — and the failure model behind it is unsupported: a universe-sync
   regression can flip `is_tradable` on one exchange, one share class or one normalisation
   pattern, which is a few percent. It is also defeated by count preservation (lost symbols
   offset by newly resolved ones) and by four successive 20% losses, which leave 41% of the
   population with no fire.

**So RegSHO gets no retention arm.** Building one needs a stated detection requirement and a
replayed `rows_resolved` series; both are named on the issue as the residual.

⚠ Dropping it also removes an entire risk class v1 had imported: no per-tape baseline
lookup, therefore no mutable denominator, no partition-scan query, no first-date-revision
hole, and no chance of the #2269 transaction trap — a baseline `SELECT` placed between the
raw-payload `conn.commit()` and `with conn.transaction()` would open an outer transaction on
a non-autocommit connection and silently degrade the per-file transaction to a savepoint
(`finra_regsho_daily_refresh.py:272`).

## 4. What ships — arms 1 and 2, lifted not copied

Both are boundaries at none-at-all and need no derivation. FINRA documents the file, not our
match against it, so where a number would be needed there is no published formulation — and
these two need no number.

1. **`skipped_invalid_row > 0`.** The parser's own predicate is `len(parts) != 6` on a bare
   `split("|")` (`finra_regsho_ingest.py:168-170`), so the healthy value is exactly 0 for a
   file with the documented six-column layout. Cited from the parser, not from "well-formed".
2. **`rows_resolved == 0` on a non-empty parse.** `rows_parsed == 0` returns early, which is
   `FNRA`'s documented empty body.

⚠ **Neither arm is claimed to be complete, and the limits are stated rather than implied.**
`skipped_invalid_row` does not catch a syntactically valid row with a wrong symbol, swapped
numeric columns or an invalid market code; header/footer/date corruption raises a
**file-level** failure instead and is already surfaced by the `RuntimeError` partial-failure
contract. Arm 2 fires on a valid non-empty file that happens to contain only instruments
outside our universe — on a sparse tape that is reachable, so the finding says "resolver or
universe" rather than asserting a break.

⚠ **Coverage genuinely lost by deleting the pooled warning, stated plainly:** a moderate
resolved-row loss (e.g. 510 → 490 of 1,000 parsed) crossed the old 0.50 and fires nothing
now. That is not a regression in *detection*, because the old arm fired on 38% of days with
no relationship to anything wrong and a fire therefore carried no information — but it is a
gap, and it is the same gap §3's residual would close.

**Lift, do not copy** — #1955's sibling-drift class; a second copy is how it recurs.

- New `app/services/finra_ingest_sentinels.py`: `SentinelFinding(key: str, kind: str,
  detail: str)` and a pure `evaluate_ingest_sentinels(*, key, failed, rows_parsed,
  rows_resolved, skipped_invalid_row, previous, retention_floor)`. `retention_floor=None`
  disables arm 3 entirely — the arm is **absent**, not set to a permissive number, so nobody
  later reads a loose constant as a calibrated one.
- `finra_short_interest_refresh.evaluate_file_sentinels` becomes a thin adapter: same
  signature, same `SentinelFinding(settlement_date=…)` return (its logger reads
  `.settlement_date`), passing `retention_floor=_RESOLVED_RETENTION_FLOOR`. Its derivation
  block stays put — it is correct for that corpus.
- `finra_regsho_daily_refresh`: delete the pooled warning; evaluate the arms **per tape**
  over `stats_list`, already keyed `(trade_date, prefix)` (`finra_regsho_ingest.py:78-80`).
  The pooled counts stay in the existing `logger.info` line as context. No DB read is added.

## 5. Tests

Pure, table-driven, no DB — mirroring `tests/test_finra_short_interest_sentinels.py`:

- each arm fires and is silent on its own axis; `failed` and `rows_parsed == 0` yield nothing;
- `retention_floor=None` yields no retention finding even with a `previous` that would
  otherwise fire — the arm is off, not loose;
- the bimonthly adapter still returns findings carrying `settlement_date`, and
  `tests/test_finra_short_interest_sentinels.py` passes **unchanged**;
- ⚠ **the boundary probe**, per the #2337 lesson: counts that divide **exactly** (900/1000 is
  the same double as `0.9`), asserted to divide exactly in the test *before* the behaviour is
  asserted, so a `<` / `<=` flip is caught. #2337's first attempt used `ceil(prev * floor)` =
  0.90010 — *near* the boundary, where both comparisons agree — and the revert probe was not
  caught;
- a per-tape regression test: a FORF-shaped file and a CNMS-shaped file in one run produce
  findings keyed to the right tape and do not pool.

## 6. Acceptance — measured

- The pooled match-rate WARNING is gone from `finra_regsho_daily_refresh`.
- `tests/test_finra_short_interest_sentinels.py` passes **unchanged** — 23 tests, the lift
  is behaviour-preserving for the bimonthly.
- **Arm 1 replayed over the stored corpus is silent.** Read from
  `filing_raw_documents.payload` and run through the **production** predicates
  (`split_regsho_body_row` / `validate_regsho_body_fields`, extracted for exactly this —
  #2337's own corollary is that a census must call the same predicate production counts,
  never a re-implementation):

  | | |
  | --- | ---: |
  | payloads replayed | **540** |
  | prefixes covered | **6 of 6** — CNMS, FNQC, FNRA, FNSQ, FNYX, FORF, 90 dates each |
  | body rows | **3,581,562** |
  | `skipped_invalid_row` | **0** |

  So the arm ships with a measured false-positive rate of zero on the whole corpus, which
  is the thing the removed arm never had. ⚠ This required the payload replay: stored
  observation counts cannot recover it (§2), and had this returned non-zero the arm would
  have been a new always-on alarm — the #2337 defect under a new name.

- **`FNRA` is fetched and stored on all 90 dates and its body is empty on every one of
  them** (0 of 90 with a non-empty body), which is why it has no observations. So the
  sixth feed is covered by the replay rather than assumed absent, and arm 2 cannot fire on
  it — `rows_parsed == 0` returns early.
- Arm 2 is silent on the corpus by construction: all five body-carrying tapes have stored
  observations on all 90 dates, so none of them resolved zero rows.
- ⚠ Reproduce the replay with the snippet recorded on #2795; it is a read-only loop over
  `filing_raw_documents` and takes well under a minute. No derived statistic is hardcoded
  in a docstring or an output string.
