/**
 * Vitest config for the eBull frontend (#92).
 *
 * Kept separate from `vite.config.ts` so the production build config does
 * not pull in vitest types. We `mergeConfig` against the existing vite
 * config so the `@/` alias and the React plugin stay in lockstep — a
 * mismatch between build-time and test-time alias resolution is one of
 * the silent failure modes called out on the issue.
 *
 * Notes:
 *  - environment: jsdom — required for React Testing Library.
 *  - setupFiles: wires `@testing-library/jest-dom` matchers and the
 *    minimal browser-API stubs (IntersectionObserver, ResizeObserver,
 *    matchMedia) that jsdom does not implement.
 *  - globals: false — every test imports `describe` / `it` / `expect`
 *    explicitly. Matches the project's no-magic-globals posture and
 *    keeps test files self-describing under strict TS.
 */
import { defineConfig, mergeConfig } from "vitest/config";

import viteConfig from "./vite.config";

export default mergeConfig(
  viteConfig,
  defineConfig({
    test: {
      environment: "jsdom",
      globals: false,
      setupFiles: ["./src/test/setup.ts"],
      css: false,
      include: ["src/**/*.{test,spec}.{ts,tsx}"],
      // Pin pool + cap concurrency. Vitest v2 defaults to forks already;
      // pinning makes the choice explicit so a future edit doesn't silently
      // switch back to threads (where #399 hit a tinypool crash).
      pool: "forks",
      poolOptions: { forks: { minForks: 1, maxForks: 2 } },
    },
  }),
);
