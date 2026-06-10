# #1417 — frontend toolchain: vite 5→6 + vitest 2→4

## Problem

`GHSA-5xrq-8626-4rwp` (critical, `vitest <4.1.0`, arbitrary file read via Vitest UI
server) fails the repo-wide `supply-chain` CI check. Currently muted via
`pnpm.auditConfig.ignoreGhsas` in `frontend/package.json` — the mute masks the
advisory instead of fixing it. `vitest@4.1.x` peers `vite ^6 || ^7 || ^8`; vitest 3.x
does not satisfy the advisory; there is no vite-5-compatible fix (empirically:
vitest 4 on vite 5 fails at startup with `ERR_PACKAGE_PATH_NOT_EXPORTED:
vite/module-runner`). So the fix is a vite major upgrade that touches the
production build.

## Change

`frontend/package.json`:

| dep | from | to | why |
| --- | --- | --- | --- |
| `vite` | `^5.4.8` | `^6.4.3` | vitest 4 peer floor; issue scopes 5→6 (not 7) |
| `vitest` | `^2.1.8` | `^4.1.8` | advisory fix is `>=4.1.0` |
| `@vitejs/plugin-react` | `^4.3.2` | `^4.7.0` | 4.7.0 peers `vite ^6`; stays on major 4 (no plugin migration) |
| `pnpm.auditConfig.ignoreGhsas` | `["GHSA-5xrq-8626-4rwp"]` | **removed** | advisory fixed for real; mute must not outlive it |

Unchanged on purpose: `jsdom ^25` (vitest 4 peer is `*`), `vitest-axe ^0.1.0`
(peer `vitest >=0.16.0`), `typescript`, `tailwindcss`/`postcss` (postcss config is
plain JS — vite 6's postcss-load-config v6 change only affects TS/YAML configs),
react stack. Node: `.nvmrc` 22 satisfies both vite 6 and vitest 4.

`frontend/vitest.config.ts`:

- vitest 4 removed `poolOptions` AND `minWorkers` (Codex ckpt-1 finding; see
  <https://vitest.dev/guide/migration.html#pool-rework>) →
  `poolOptions.forks.{minForks:1,maxForks:2}` becomes top-level `maxWorkers: 2`
  only — there is no `minWorkers` equivalent. `pool: "forks"` stays (still
  valid; pins the #399 tinypool decision explicitly).

`frontend/src/test/vitest-axe.d.ts`:

- shim currently augments vitest 2.x `Assertion`/`AsymmetricMatchersContaining`.
  If `pnpm typecheck` breaks under vitest 4, re-target the v4 augmentation point
  (`Matchers` interface). Decided empirically by the typecheck gate, not guessed.

No app-code changes expected. Vite 6 migration-guide breaks checked against
this repo one-by-one (Codex ckpt-1 asked for the explicit list): no custom
`resolve.conditions` / `ssr.resolve.conditions` (default-change doesn't apply);
no Sass/SCSS (modern-API default n/a); no library mode (CSS filename change
n/a); no `server.proxy.bypass` usage (plain target+rewrite only); no custom
glob ranges in imports; postcss config is plain JS (postcss-load-config v6
TS/YAML loader change n/a); no SSR (module-runner / hotUpdate changes n/a);
no vite plugins beyond `@vitejs/plugin-react`.

## Risks

- vitest 4 mocking-behaviour changes (mock reset semantics, module spies) can
  break existing tests without any config error → full `pnpm test` (including
  SetupPage integration) is the gate, not `test:unit`.
- vite 6 prod build output could differ → verified by `pnpm build` + manual
  smoke of the BUILT app (`vite preview`, which inherits `server.proxy` for
  `/api` → backend :8000), not just dev server.
- CI pnpm pins (10.33.0 supply-chain / 10 frontend-ci) vs local 11.0.6: lockfile
  version 9.0 is compatible across both; `--frozen-lockfile` install in CI is
  the proof gate.

## Verification (Definition of Done for this PR)

1. `pnpm --dir frontend typecheck` clean.
2. `pnpm --dir frontend dark:check` clean.
3. **Full** `pnpm --dir frontend test` (not `test:unit`) green.
4. `pnpm --dir frontend build` succeeds; bundle produced.
5. Manual smoke of built app via `vite preview` against live dev backend —
   page renders, no console errors, an API-backed view loads data. NOTE
   (Codex ckpt-1 raised; resolved empirically): `vite preview` on 6.4.3 DOES
   inherit `server.proxy` — `curl http://localhost:4173/api/system/status`
   returned the backend's 401 (proxied), and an authed
   `/api/instruments/AAPL/ownership-rollup` through :4173 returned real data.
   No `preview.proxy` block needed.
6. `pnpm --dir frontend audit --audit-level high` clean WITHOUT the GHSA mute.
7. `pnpm --dir frontend install --frozen-lockfile` passes after lockfile
   refresh (CI supply-chain gate replay). pnpm-version skew checked: pnpm
   self-pins to `packageManager: pnpm@10.30.3` inside `frontend/` (verified:
   `pnpm --version` there returns 10.30.3 despite global 11.0.6), lockfile
   stays `lockfileVersion: '9.0'` — readable by CI's pinned 10.33.0.
