# Copy-mirror closed-position history (#1927)

Status: live spec. Operator-approved schema shape (issue #1927, 2026-07-04).

## Problem

`_sync_mirrors` step 3a (`app/services/portfolio_sync.py:399-409`) **hard-DELETEs**
`copy_mirror_positions` rows that vanished from the freshly-parsed mirror payload —
i.e. positions the copied trader closed while we were still copying them — with no
archive. The copy view therefore reads as "buy-only": you see the current open book,
never the exits. Own positions do NOT lose this history: `_upsert_broker_positions`
archives the disappeared row to `broker_positions_closed` in the SAME transaction,
immediately before its sweep-DELETE (`portfolio_sync.py:235-256`, table `sql/194`).

## Source rule

This is not an external-data-treatment decision; it is our own settled invariant:
the **own-positions closed-history archive** (`broker_positions_closed`,
`LIKE broker_positions INCLUDING DEFAULTS` + `closed_detected_at` + PK
`(position_id, closed_detected_at)`, sql/194; archive-before-delete at
portfolio_sync.py:235-256). This ticket mirrors that pattern for copy mirrors —
same shape, same invariants, no novel modeling.

## Scope (falsified on dev DB 2026-07-04)

- Only step-3a (per-position eviction while still copying) loses history. Confirmed:
  the DELETE at 399-409 has no preceding archive INSERT.
- Whole-mirror disappearance (step 4, `portfolio_sync.py:470-506`) **soft-closes**
  the mirror (`active=FALSE, closed_at`) and by design **retains** its nested
  positions for audit (migration 022 header). So archiving only at step 3a is both
  necessary and sufficient — a soft-closed mirror's positions are not deleted.
- **Re-copy edge (Codex ckpt-1):** step 2 reactivates a re-copied mirror
  (`active=TRUE, closed_at=NULL`, `portfolio_sync.py:352/367`) BEFORE step 3a evicts.
  A soft-closed mirror's *retained* stale positions (from the prior copy episode)
  that are absent from the new payload would then be archived at re-copy time, stamped
  `closed_detected_at=now` — mislabelling audit residue as fresh exits (and we don't
  hold their true close time). Guard: **only archive when the mirror was already
  active at the start of this sync.** Capture `active_before = {mirror_id : active}`
  once at the top of `_sync_mirrors`; a newly-appearing or reactivated mirror is not
  in `active_before` → skip its step-3a archive (its evictions are either nothing or
  stale residue, deleted silently as today). A genuinely ongoing mirror IS in
  `active_before` → an eviction is a real close → archive it.
- Dev DB: 2 mirrors, 434 `copy_mirror_positions`, archive table absent, 0 rows in
  `broker_positions_closed` (own-positions archive also empty here — expected, no
  external closes observed in dev).

## Design

### 1. Schema — `sql/214_copy_mirror_closed_positions.sql`

```sql
CREATE TABLE IF NOT EXISTS copy_mirror_closed_positions (
    LIKE copy_mirror_positions INCLUDING DEFAULTS,
    closed_detected_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (mirror_id, position_id, closed_detected_at)
);
CREATE INDEX ... ON copy_mirror_closed_positions (mirror_id, closed_detected_at DESC);
```

- `LIKE ... INCLUDING DEFAULTS` (not `INCLUDING ALL`): copies columns + defaults but
  NOT the FK `mirror_id REFERENCES copy_mirrors ON DELETE CASCADE`. Intentional —
  history must survive even if a mirror row is ever hard-deleted (matches
  `broker_positions_closed`, which also uses bare `LIKE ... INCLUDING DEFAULTS`).
- PK includes `mirror_id` (position_id is only unique per-mirror) and
  `closed_detected_at` (a position may close → re-copy → close again). Mirrors the
  own-positions `(position_id, closed_detected_at)` PK.
- Index on `(mirror_id, closed_detected_at DESC)` for the read path (recent exits
  per mirror).

### 2. Service — archive before the step-3a DELETE

In `_sync_mirrors`, immediately before the DELETE at `portfolio_sync.py:399-409`,
INSERT the about-to-be-evicted rows into the archive, in the same transaction,
listing columns explicitly (matches `broker_positions_closed` archive, not `SELECT *`):

```sql
INSERT INTO copy_mirror_closed_positions
    (mirror_id, position_id, parent_position_id, instrument_id, is_buy, units,
     amount, initial_amount_in_dollars, open_rate, open_conversion_rate,
     open_date_time, take_profit_rate, stop_loss_rate, total_fees, leverage,
     raw_payload, updated_at, closed_detected_at)
SELECT mirror_id, position_id, parent_position_id, instrument_id, is_buy, units,
       amount, initial_amount_in_dollars, open_rate, open_conversion_rate,
       open_date_time, take_profit_rate, stop_loss_rate, total_fees, leverage,
       raw_payload, updated_at, %(now)s
FROM copy_mirror_positions
WHERE mirror_id = %(mirror_id)s
  AND position_id <> ALL(%(position_ids)s::bigint[])
ON CONFLICT (mirror_id, position_id, closed_detected_at) DO NOTHING
```

`now` is the sync-cycle timestamp already threaded into `_sync_mirrors`. `ON CONFLICT
DO NOTHING` makes a re-run within the same cycle idempotent (mirrors own-positions).
Gate the archive INSERT on `mirror.mirror_id in active_before` (re-copy guard above);
the DELETE runs unconditionally as today.

### 2b. Test-infra (Codex ckpt-1)

The archive table has no FK to the copy cluster (bare `LIKE`), so `TRUNCATE
copy_mirror_positions, copy_mirrors, copy_traders ... CASCADE` will NOT reach it. Add
`copy_mirror_closed_positions` to: (a) `_PLANNER_TABLES` near the copy cluster in
`tests/fixtures/ebull_test_db.py:273`, and (b) both TRUNCATE statements in
`tests/test_portfolio_sync_mirrors.py:53` (and the shared mirror fixture at :48-56),
or archive rows leak across DB tests.

### 3. Bootstrap-tier note

Per `feedback-backfills-belong-in-bootstrap`: pre-table closes are **unrecoverable** —
`closed_positions_net_profit` is an aggregate delta, not per-position history, so
there is nothing to seed from. The archive starts at deploy time; that loss is
accepted and documented (operator condition 2 on the issue). No backfill job.
`copy_mirror_closed_positions` is born-empty and fills forward as the sync observes
closes — nothing to add to bootstrap stages.

### 4. Read surface — `app/api/copy_trading.py::get_mirror_detail`

Add `closed_positions: list[MirrorClosedPositionItem]` to `MirrorDetailResponse`
(NOT `MirrorSummary` — it's detail-only, keeps the list endpoint cheap). Each item is
a realized exit: `position_id, instrument_id, symbol, company_name, is_buy, units,
amount, open_date_time, closed_detected_at`. No MTM and **no `open_rate`** — the
position is gone; we know entry size + observed-closed time but NOT the trader's exit
price, so we surface "closed" as an event, never a fabricated realized P&L.
**Currency contract (Codex ckpt-1):** `amount` is native USD in the archive; convert
it to `display_currency` via `_convert_usd` exactly as open positions do
(`copy_trading.py:149`). We drop `open_rate` (a native price) from the item to avoid
mixing native and display figures in one row. Query the archive for the mirror,
most-recent first (`ORDER BY closed_detected_at DESC`), LIMIT a sane cap (e.g. 100).

### 5. FE — `frontend/src/pages/CopyTradingPage.tsx`

Add a "Recent exits" `Section` below "Open positions", listing the archived closes
("Trader closed <units> <symbol>, opened <date>, exit observed <date>"). Empty state:
"No copied-position exits observed yet." Extend `MirrorDetailResponse` +
`MirrorClosedPositionItem` in `frontend/src/api/types.ts` field-for-field from the
Pydantic model (prevention-log L474).

## Tests

- Pure/DB: `_sync_mirrors` archives an evicted position before delete; re-run same
  cycle is idempotent (ON CONFLICT); a position present in the new payload is NOT
  archived; whole-mirror soft-close does not archive/delete its positions.
- Endpoint: `get_mirror_detail` returns archived exits most-recent-first.

## Verification (DoD ETL clauses)

- Migration applies on dev DB; table + index present.
- `_sync_mirrors` exercised via the copy-trading sync path (or a targeted DB test on
  the 2 dev mirrors) — evicted row lands in archive.
- `/portfolio/copy-trading/{mirror_id}` renders `closed_positions` (empty on dev
  until a real close is observed — honest empty state).
- Operator spot-check of one mirror vs the eToro app after first sync (transferred
  from #1915) — noted on the PR as operator-follow-up (loop env has no broker creds).
