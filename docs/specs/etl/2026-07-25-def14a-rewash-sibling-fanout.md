# DEF 14A rewash: the Item 403 arm must fan out over share-class siblings

Issue: #2157. Service: `app/services/rewash_filings.py::_rewash_def14a`.

## Source rule

Not an SEC-interpretation question — this is one of our own settled invariants,
so the governing rule is the **prevention-log entry on rewash sibling fan-out**
(`docs/review-prevention-log.md`, "#2105 def14a comp rewash"), which states it
exactly:

> a rewash/backfill that re-runs a parser must call the **SAME apply chokepoint
> the live path uses** (`upsert_filing`, `upsert_form_3_filing`,
> `_record_def14a_observations_for_filing` — the sibling fan-out is inside),
> never a slimmed per-instrument variant.

That entry was written for the **Item 402(c) exec-comp** arm of this very
function and fixed there (#2105 / PR #2106). The **Item 403 beneficial-ownership
arm of the same function was left on the slimmed per-instrument path**, so the
lesson is being re-learned on the sibling defect it was written to prevent.

The upstream data rule it rests on **is** an SEC one. Reg S-K **Item 403(a)**
(17 CFR § 229.403(a)) requires the **registrant** to disclose beneficial
ownership of "**any class** of the registrant's voting securities", and
**403(b)** requires management holdings "of **each class**". The disclosure is
therefore **registrant-level**, and a share class is a class *of* that
registrant — not a separate registrant. One issuer CIK files one proxy covering
all its classes, so every share-class sibling instrument we model
(GOOG/GOOGL, HEI/HEI.A) is entitled to the same Item 403 rows from the same
accession. `def14a_ingest.py:1054` fans the live write over
`siblings_for_issuer_cik(conn, issuer_cik)` for exactly that reason, and the
comp helper's docstring records the identical argument for Item 402.

## Premise check — the ticket understates the scope by ~5×

The ticket reports **66 stale rows / 11 instruments** in
`ownership_def14a_current` and hypothesises that "the sibling set resolved at
rewash time differs from the set at original ingest".

The hypothesis is directionally right and mechanically wrong. Re-derived at
`0fdb92f3` (post-v8 rewash):

- The 11 accessions the ticket's regex finds split into **two unrelated
  classes**. Six (`METCB`, `HEI.A`, `BELFB`, `GOOG`, `BELFA`, `COHR.US`) are the
  sibling defect; the other five (`KIM`, `LAUR`, `LYB`, `CERS`, `CVSA`) are
  guard-blocked accessions still at v6 — #2158's territory, not this ticket's.
- Every affected accession is a plain share-class sibling set: GOOG/GOOGL,
  HEI/HEI.A, BELFA/BELFB, METC/METCB, UA/UAA, FWONA/FWONK, LILA/LILAK/LILAP,
  GEF/GEF.B, MKC/MKC-V, BIO/BIO-B, XRX/XRXDW, HTZ/HTZWW, OPEN/OPENL/OPENW/OPENZ.
  This is an enumeration of all 35 affected accessions, not a sample — but it is
  **not** a proof that the resolver never drifts, and the design does not rest on
  one. The union arm below makes drift irrelevant by construction: any
  instrument that ever held rows for the accession is written regardless of
  whether the resolver still returns it.
- The real cause is simpler and worse: **the Item 403 rewash arm never fans out
  at all.**

Full-population, the true blast radius:

| measure | count |
| --- | --- |
| orphaned `(accession, instrument)` pairs — live observations, **zero** typed rows | **41** |
| instruments affected | **31** |
| accessions affected | **35** |
| live `ownership_def14a_observations` rows with no typed backing | **457** |
| `ownership_def14a_current` rows behind them | **331** |

The ticket's 66 rows are only the subset whose stale text still matches the
pre-v7 shape regex (numeric name or interior newline). The other ~265 `_current`
rows are equally stale but shape-clean, so the regex cannot see them.

## Defect

`_rewash_def14a` (`rewash_filings.py:~610-650`) does three things at three
different scopes:

```python
# 1. accession-wide DELETE — clears EVERY sibling's typed rows
cur.execute("DELETE FROM def14a_beneficial_holdings WHERE accession_number = %s", (acc,))

# 2. re-INSERT for the ONE resolved instrument
for holder in parsed.rows:
    _upsert_holding(conn, ..., instrument_id=int(instrument_id), ...)

# 3. observations + _current refresh, also for the ONE resolved instrument
_record_def14a_observations_for_filing(conn, instrument_id=int(instrument_id), ...)
refresh_def14a_current(conn, instrument_id=int(instrument_id))
```

`instrument_id` comes from `SELECT issuer_cik, instrument_id FROM
def14a_beneficial_holdings WHERE accession_number = %s LIMIT 1` — an
**arbitrary** sibling (no `ORDER BY`), which is why the surviving instrument
varies per accession in the table above (GOOGL for one Alphabet proxy, GOOG for
the next).

Net effect per rewashed accession with siblings:

1. Step 1 deletes the sibling's typed rows; step 2 never re-inserts them →
   the sibling's `def14a_beneficial_holdings` rows are **gone**, and the rollup /
   drillthrough / drift readers that use that table (#2140 D13) lose them.
2. Step 3's tombstone is correctly scoped to `(instrument_id,
   source_document_id)` — #2140 made it per-instrument **precisely because the
   live writer fans out** — but the rewash never performs the other siblings'
   writes, so their observations stay `known_to IS NULL` with the **old parser's
   names**, and `refresh_def14a_current` is never called for them. Their
   `_current` rows survive every rewash forever.

The per-instrument tombstone is therefore not the bug; it is the correct half of
a contract whose other half (the fan-out) is missing. Widening the tombstone to
accession scope would be the wrong fix — it would make each sibling's write
supersede the previous sibling's rows, the exact trap #2140 documented.

## Design

Mirror `_rewash_exec_comp_all_instruments` (`rewash_filings.py:688`), which
already solves this for the Item 402(c) arm and whose shape is settled:

**Instrument set** = `_resolve_siblings(conn, instrument_id=resolved, issuer_cik=cik)`
**UNION** every instrument that already holds rows for this accession, in either
`def14a_beneficial_holdings` **or** live `ownership_def14a_observations`, **UNION**
the resolved instrument.

The union arm is load-bearing twice over, and the observations half is new
relative to the comp helper:

- an instrument that has since **left** the sibling set is refreshed rather than
  frozen on the old parser (the comp helper's stated reason);
- the 41 already-orphaned pairs have **no typed rows left to find** — the
  accession-wide DELETE removed them — so a typed-table-only union would never
  reach them and the repair rewash would not fix the existing damage. Their live
  observations are the only remaining evidence that the instrument was ever
  written, which is why the observations table joins the union.

`_resolve_siblings` already fails safe: sentinel or empty result → `[resolved]`.
Its `ValueError` on a non-numeric CIK is caught the same way the comp helper
catches it — log, fall back to the known instruments, never fail the accession
(freezing rows on the old parser is the bug being fixed).

Then run steps 2 and 3 **per instrument in the set**, matching the live
chokepoint's body (`def14a_ingest.py:1061-1119`) call for call:

```python
for iid in sorted(instrument_ids):
    for holder in parsed.rows:
        _upsert_holding(conn, ..., instrument_id=iid, ...)
    _record_def14a_observations_for_filing(conn, instrument_id=iid, ...)
    refresh_def14a_current(conn, instrument_id=iid)
    esop_written = _record_esop_observations_for_filing(conn, instrument_id=iid, ...)
    if esop_written > 0:
        refresh_esop_current(conn, instrument_id=iid)
```

The accession-wide DELETE at step 1 stays as-is and becomes **correct** once the
loop re-inserts every sibling — it is what stops a holder dropped by the new
parser from lingering under any sibling.

### Two further arms of the same defect (Codex ckpt-1)

The prevention rule is "call the same chokepoint", so the audit is *every* write
the live path makes, not only the one the ticket noticed. Two more diverge:

**ESOP write-through is absent entirely — not merely un-fanned.**
`rewash_filings.py` contains **zero** references to `esop`: the rewash never
calls `_record_esop_observations_for_filing` / `refresh_esop_current`, which the
live path runs per sibling (`def14a_ingest.py:1105`, manifest parser `:453`). So
an ESOP plan renamed by a parser fix stays stale under **every** instrument
forever, and a plan the improved parser newly detects is never recorded. Live
population is small — 26 live rows / 14 instruments / 23 accessions — which is
why it went unnoticed, not a reason to leave it: the whole point of a rewash is
that the corrected parse reaches storage. Added to the per-instrument loop
above.

**Drift re-check is fanned on the live path, single-instrument on rewash.**
`rewash_filings.py:665` passes `instrument_ids=[int(instrument_id)]`; the live
paths pass `siblings` (`def14a_ingest.py:1134`, manifest parser `:528`). So
`def14a_drift_alerts` keeps the same sibling stale-state class this ticket
exists to remove. Changed to pass the resolved instrument set.

Both stay **best-effort / savepoint-isolated** exactly as they are today — this
ticket widens the instrument set they run over, and changes nothing about their
failure contract.

### Ordering + locking

The existing `acquire_filing_accession_write_lock(conn, accession)` is taken
before the DELETE and covers the whole fan-out, so the widened write set is
already serialised against a concurrent live `_upsert_holding` (#817). No new
lock.

### Not in scope

The latest-N primary cap (`def14a_within_cap`) gates only the **rescue** path
(no typed rows at all). The happy path deliberately skips it because it operates
on already-existing typed rows (spec §6.3 — existing rows untouched). Fanning
out does not change which accessions are in scope, only which instruments of an
in-scope accession get written, so the cap decision is unchanged.

## Verification

### Full-population, against stored rows

The invariant is expressible directly in SQL and must hold at **zero** after the
backfill — a full-population check, not a panel:

```sql
-- live observations with no typed backing for the same (accession, instrument)
SELECT count(*) FROM (
  SELECT o.source_document_id, o.instrument_id
  FROM ownership_def14a_observations o
  WHERE o.known_to IS NULL AND o.source = 'def14a'
  GROUP BY 1, 2
) o
LEFT JOIN (
  SELECT accession_number, instrument_id FROM def14a_beneficial_holdings GROUP BY 1, 2
) h ON h.accession_number = o.source_document_id AND h.instrument_id = o.instrument_id
WHERE h.instrument_id IS NULL;
-- before: 41   after: 0
```

That query only catches instruments that already had observations. It cannot see
a **resolver-only sibling** — one the resolver returns but which never had rows
under this accession at all, so neither side of the join mentions it. The
coverage assertion has to be driven from the resolver's own output:

```sql
-- every sibling of every rewashed accession must hold typed rows for it
WITH washed AS (
  SELECT DISTINCT h.accession_number, h.issuer_cik
  FROM def14a_beneficial_holdings h
  JOIN filing_raw_documents f
    ON f.accession_number = h.accession_number
   AND f.document_kind = 'def14a_body'
   AND f.parser_version = <current version>
),
expected AS (
  SELECT w.accession_number, e.instrument_id
  FROM washed w
  JOIN external_identifiers e
    ON e.identifier_type = 'sec/cik'
   AND e.identifier_value = w.issuer_cik
)
SELECT count(*) FROM expected x
LEFT JOIN def14a_beneficial_holdings h
  ON h.accession_number = x.accession_number AND h.instrument_id = x.instrument_id
WHERE h.instrument_id IS NULL;
-- must be 0 after the backfill
```

The exact sibling projection is whatever `siblings_for_issuer_cik` uses — the
query above is pinned to that function's source at implementation time rather
than re-derived, per the prevention-log rule that a repair script must mirror
the live resolver rather than hand-roll one.

and the ticket's own query:

```sql
SELECT count(DISTINCT source_document_id), count(DISTINCT instrument_id), count(*)
FROM ownership_def14a_current
WHERE shares IS NOT NULL
  AND (holder_name ~ '^[0-9][0-9,.\s]*$' OR holder_name LIKE E'%\n%');
-- before: 11 | 11 | 66
-- after:  the 6 sibling accessions clear; the 5 guard-blocked ones remain
--         (they are #2158's residual, not this ticket's)
```

The expected end state is **not zero** here, and saying so up front matters: a
reviewer seeing 5 accessions left must be able to tell a partial fix from the
correct one.

### Sibling coverage (ETL clauses 8, 11)

Panel — the sibling pairs above, checked on **both** classes, which is the whole
point:

- `GOOG` **and** `GOOGL` (`0001308179-25-000511`, `0001308179-26-000342`)
- `HEI` **and** `HEI.A` (`0001140361-25-002543`)
- `BELFA` **and** `BELFB` (`0001437749-25-011718`, `0001437749-26-011998`)
- `METC` **and** `METCB` (`0001213900-26-047772`)
- `UA` **and** `UAA` (`0001336917-25-000112`)

For each: `/instruments/{symbol}/ownership-rollup` returns the same DEF 14A
holder set on both classes, and no numeric/newline holder names remain.

### Cross-source (ETL clause 9)

Alphabet `0001308179-25-000511` — the Item 403 holder set read from the primary
document under `https://www.sec.gov/Archives/edgar/data/1652044/000130817925000511/`
must match what both `GOOG` and `GOOGL` render.

### Backfill (ETL clause 10)

`PYTHONPATH=. uv run python scripts/rewash.py --kind def14a_body` after a parser
version bump, so every accession is re-driven through the fixed fan-out.

### Tests

Pure-logic where possible; one DB-backed integration test for the fan-out itself
(this is a genuinely new SQL mechanism in this arm, and the write set is the
thing under test):

- the instrument set unions siblings, typed-row holders, **and live-observation
  holders** — the last is the arm that reaches the 41 already-orphaned pairs;
- a rewash of an accession with two siblings leaves typed rows for **both**;
- a sibling's stale observations are superseded and its `_current` refreshed;
- non-numeric issuer CIK falls back to the known instruments instead of failing
  the accession.
