# OpenFIGI key pre-flight nudge (#1344)

## Problem
OpenFIGI CUSIP resolution is materially faster when `OPENFIGI_API_KEY` is
set. The resolver already adapts batch size to key presence
(`openfigi_resolver.py:266,316`), so the code path is correct. The gap is
UX: operators don't learn the key matters until bootstrap stage S13
(`cusip_resolver_post_bulk_sweep`) crawls.

## Source rule / cited numbers
Not a data-treatment decision — no SEC reg applies. Two distinct figures,
both already documented; cite the correct one for the correct claim:
- **Rate limit = 100×** (250 vs 25,000 mappings/min — settled-decision 635).
- **Wall-clock ≈ 10×** on a real backlog (`app/config.py:88-92` comment:
  unkeyed ~48 min vs keyed ~5 min on a ~12k unresolved-CUSIP backlog).
Operator-facing copy cites the **wall-clock** figure ("~10× faster CUSIP
resolution"), the honest end-to-end number, NOT the 100× rate limit.
This is a quote of an existing documented estimate, not a new measured
perf claim → does not trip perf-claim-lint (no `perf` label / no
`## Performance impact` header in the PR).

## Placement decision
The issue offers "first-install setup page **or** pre-bootstrap checklist".
`/setup` is pre-auth and cannot read env state; `/system/bootstrap/status`
needs a session. The honest home is the post-auth dashboard pre-bootstrap
surface (`AppShell`, beside the existing `BootstrapNudgeBanner`), shown only
while bootstrap has not started — exactly when setting the key still helps.

## Backend
`BootstrapStatusResponse` (app/api/bootstrap.py) gains
`openfigi_key_present: bool = bool(settings.openfigi_api_key)`.
**Boolean only — never the key value.** Citation: ADR 0001
("Decryption is server-side only. Plaintext never leaves the backend …
the UI sees metadata only", lines 142–148). Presence is metadata; the
secret stays server-side. Computed once in `_build_status_response`, set
on both return paths (snap-None and snap-present).

Config-read semantics: `settings = Settings()` is process-global at import;
`.env`/env changes are NOT live-reloaded. So `openfigi_key_present`
reflects the key as of **API process start**. Crucially the resolver reads
the *same* process-global `settings.openfigi_api_key` (`from_env`), so the
banner's "no key" exactly predicts what S13 will run with — no false nudge.
After adding the key the operator restarts the API; banner copy says so.

Frontend `BootstrapStatusResponse` interface gains the same field.

## Frontend
New `OpenFigiKeyNudgeBanner.tsx` (sibling of `BootstrapNudgeBanner`,
rendered in `AppShell`). Shows when ALL hold:
- `openfigi_key_present === false`, AND
- a (re-)run may still execute S13 (`cusip_resolver_post_bulk_sweep`)
  without a key:
  - `pending` — fresh install, full run ahead; OR
  - `partial_error` AND S13 has not terminally succeeded. A
    `partial_error` can originate in a different/later lane while S13
    already finished; retry reruns only failed + later-same-lane stages,
    so a succeeded S13 will NOT rerun and the key can no longer help
    (Codex ckpt-2). Read the S13 stage from `stages[]` and hide when its
    status is `success`/`skipped`.
  `running` (mid-run, too late this run) and `complete` (re-run from a
  healthy system is a deliberate operator action) are excluded, AND
- not dismissed (localStorage, persistent — advisory nudge; a deliberate
  operator "no key" choice should stick across reloads; distinct from the
  bootstrap banner's intentionally non-permanent sessionStorage dismiss).

Copy: recommends setting `OPENFIGI_API_KEY` **and restarting the API**
before bootstrap; "~10× faster CUSIP resolution" (wall-clock, per
`config.py`); link to https://www.openfigi.com/api. Dismissible.
Polls /system/bootstrap/status at 60s (mirrors sibling) so it self-hides
when bootstrap leaves the pending/partial_error window; a newly-set key is
reflected after the operator restarts the API (config is not hot-reloaded).

## Out of scope (deferred)
- Drift-heal re-surface if S13 runs >2 min with no key (acceptance bullet 3).
  Requires hooking live stage-timing; file as tech-debt follow-up. The
  pre-flight nudge covers the primary "operator never knew" gap.

## Tests
- Backend: status endpoint returns `openfigi_key_present` reflecting
  `settings.openfigi_api_key`. Assert BOTH return paths carry the field —
  `snap is None` (no run yet) and `snap` present (run exists) — and both
  true/false via monkeypatch on the settings singleton.
- Frontend: banner renders when (pending|partial_error) + no key + not
  dismissed; hidden when key present, when status running/complete, and
  when dismissed; dismiss persists to localStorage. Add the new field to
  any shared bootstrap-status test fixture/type so stale mocked payloads
  fail loudly (schema backcompat).

## Verification
- Hit `/system/bootstrap/status` on dev, confirm new field renders.
- Load dashboard with key unset → nudge shows; dismiss → gone on reload.
