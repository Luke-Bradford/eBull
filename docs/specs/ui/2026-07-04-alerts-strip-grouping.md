# AlertsStrip grouping + severity tiers (#1898)

## Problem (verified on live dev)
`GET /alerts/guard-rejections` returns 18 rows, 15 identical `kill_switch` (4 symbols ×
several days) + 3 `auto_trading`. The strip renders one row per `(symbol × day)` for the
same root cause → "18 new, zero information", and real actionable alerts would be buried.
Position + coverage feeds are empty in dev but the same emission-per-row shape applies.

## Scope
FE-only. Regroup the three existing feeds by root cause + tier by severity. No backend /
schema change. Items 1–3 of the ticket. Item 4 (new alert types: rank moves, thesis
staleness) needs new backend data → deferred to a follow-up ticket.

## Source-grounded group key
`app/services/execution_guard.py::_build_explanation` builds
`explanation = "FAIL — " + "; ".join(f"{rule}: {detail}")`. A `detail` may itself contain
`; ` (e.g. `auto_trading`), so re-splitting the whole string is ambiguous. The ONLY
unambiguous key is the **leading rule code**: strip the `FAIL — ` prefix, take the substring
before the first `:`. This groups the real flood correctly (`kill_switch`, `auto_trading`).
Multi-rule failures group under their first (primary) rule — acceptable for a summary card.

## Design
New pure module `frontend/src/components/dashboard/alertModel.ts`:

- `parseGuardReason(explanation): string` — leading rule code (fallback: full string).
- `GUARD_REASON_META: Record<code, {label, consequence, action:{label,to}}>` — known codes
  (`kill_switch`, `auto_trading`); unknown → humanized code, generic consequence,
  action → `/recommendations`.
- `buildAlertModel(guard, position, coverage, cursors): AlertItem[]` returning tiered items:
  - **actionable** (tier 0): position alerts (SL/TP/thesis breach) — kept per-instrument,
    each links to its instrument. These are the alerts that must never be buried.
  - **informational** (tier 1): guard rejections grouped by reason code →
    `{ id:"guard:"+code, label, symbols:string[] (unique, sorted), count, latestTs,
       unseen:boolean (any member decision_id > cursor.guard), consequence, action }`.
  - **housekeeping** (tier 2): coverage drops grouped by transition. Group key uses a `∅`
    sentinel for SQL NULL `new_status` (`"analysable→∅"`) so NULL cannot collide with a
    literal status string; display renders `old → —`. One summary card per transition.
- `GUARD_REASON_META` covers all 17 `RuleName` codes (execution_guard.py:89-107) with label +
  consequence; unknown codes fall back to humanized code + generic consequence +
  `/recommendations`. All guard groups sit in the informational tier (a blocked trade is a
  policy rejection, not a position-level action); the actionable tier is position alerts.
  - Sort: tier ASC, then `latestTs` DESC within tier.

React keys: group cards keyed by `id` (`guard:<code>` / `coverage:<transition>`), unique
within the rendered list **by construction** (Map key) — satisfies prevention-log #758/#759
(never key `.map()` by a non-unique group label). Position rows keyed by `alert_id`.

## Preserved behavior (unchanged accounting) — Codex ckpt-1 resolutions
- Header `{totalUnseen} new` pill stays = sum of per-feed backend `unseen_count` (honest count
  of unseen emissions). Grouping changes the BODY, not the count.
- **Overflow (H1):** `renderedGuard/Position/Coverage` stay = the raw fetched array lengths
  (`guard.data.rejections.length`, etc.), NOT the grouped-card count. `anyOverflow` compares
  backend `unseen_count` to raw fetched length exactly as today → unchanged. Grouping never
  feeds the overflow math.
- **Cursor advancement (H2):** `Mark all read` / `Dismiss all` compute per-feed max ids from
  the RAW feed arrays (`guard.data.rejections` → max `decision_id`, coverage → max `event_id`,
  position → max `alert_id`), never from grouped `latestTs`. BIGSERIAL-id ordering preserved
  (clocks can skew). Groups carry `maxId` ONLY to drive the unseen highlight.
- Unseen highlight: a guard/coverage group is "unseen" if any member id > that feed's cursor;
  position rows keep current per-alert `alert_id > cursor.position` semantics.
- Backend feeds/cursors/`snapshot_read` (guard-only today) are untouched — no backend change.

## Hard-safety deviation from ticket
Ticket direction 1 shows a `[Deactivate]` button on the kill-switch card. Wiring a
kill-switch mutation is forbidden (autonomy hard-safety rule). The kill-switch card instead
links to **/admin** ("Manage in Admin"), where the operator deactivates. Documented in PR.

## Tests
- `alertModel.test.ts` (pure, no DB): `parseGuardReason` cases (prefix present/absent,
  detail-with-semicolon, multi-rule); `buildAlertModel` collapses the 18-row fixture → 2
  guard cards with correct symbol sets/counts; tier ordering; unseen propagation.
- Update `AlertsStrip.test.tsx`: assert N identical guard rows render as ONE card with a
  symbols summary + count, actionable position alert sorts above informational guard group.
