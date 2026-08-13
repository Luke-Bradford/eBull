/**
 * Vitest setup (#92).
 *
 * Runs once per test file before any test code.
 *
 *  1. Imports `@testing-library/jest-dom/vitest` so matchers like
 *     `toBeInTheDocument()` are available on `expect(...)` and typed
 *     against vitest's `Assertion` interface (the `/vitest` entry point
 *     is what wires the type augmentation).
 *
 *  2. Registers an `afterEach(cleanup)` for `@testing-library/react`.
 *     RTL only auto-cleans when vitest `globals: true` is set; we use
 *     `globals: false` (explicit imports), so cleanup must be wired
 *     here. Without it, multiple `render()` calls in one file leak DOM
 *     state across tests.
 *
 *  3. Stubs three browser APIs that jsdom does not implement:
 *     `IntersectionObserver`, `ResizeObserver`, `matchMedia`. Tailwind
 *     utilities and several React libraries probe for these at import
 *     time. Stubbing now keeps cryptic failures away from future tests.
 */
import { afterEach, expect } from "vitest";
import { cleanup } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";
import * as axeMatchers from "vitest-axe/matchers";
// Type augmentation for `expect(...).toHaveNoViolations()` lives at
// `./vitest-axe.d.ts` and is loaded automatically via tsconfig's
// `include: ["src"]` — vitest-axe 0.1.0's bundled augmentation targets
// the removed `Vi.Assertion` namespace, so we re-augment the live
// vitest 2.x `@vitest/expect` Assertion ourselves. No runtime import
// needed; the matcher itself is registered below via `expect.extend`.

// vitest-axe (#1086 — replaces external Lighthouse + axe CLI runs with
// an in-repo a11y suite). Registers `toHaveNoViolations` on `expect(...)`
// so admin-surface tests can assert axe-core finds zero accessibility
// violations on the rendered DOM tree. jsdom does not compute CSS, so
// rules that depend on layout / contrast (color-contrast, focus-order
// when relying on layout) are skipped by axe automatically — those
// remain covered by `frontend/scripts/check-dark-classes.mjs` and the
// keyboard-nav tests in `ProcessesTable.test.tsx`.
expect.extend(axeMatchers);

afterEach(() => {
  cleanup();
});

class NoopObserver {
  observe(): void {}
  unobserve(): void {}
  disconnect(): void {}
  takeRecords(): unknown[] {
    return [];
  }
}

if (typeof globalThis.IntersectionObserver === "undefined") {
  globalThis.IntersectionObserver = NoopObserver as unknown as typeof IntersectionObserver;
}

if (typeof globalThis.ResizeObserver === "undefined") {
  globalThis.ResizeObserver = NoopObserver as unknown as typeof ResizeObserver;
}

// jsdom 25 + vitest 2 + pool='forks' returns a plain object for
// `window.localStorage` whose Storage prototype methods are missing
// (`getItem` / `setItem` / `clear` / `removeItem` / `length` all
// undefined). The theme tests at `src/lib/theme.test.tsx` and
// `src/lib/useChartTheme.test.tsx` call `window.localStorage.clear()`
// in `beforeEach` and crash on
// `TypeError: window.localStorage.clear is not a function`.
//
// Install a minimal in-memory Storage polyfill on window so the
// production code path (`window.localStorage.getItem(STORAGE_KEY)`
// in `lib/theme.tsx`) works under tests without per-test mocks.
// The polyfill resets between tests because the wrapping `Map`
// instance lives on the per-file module scope and `cleanup()` plus
// the test-file's own `beforeEach(() => window.localStorage.clear())`
// keeps state from leaking.
if (typeof window !== "undefined") {
  const memory = new Map<string, string>();
  const polyfill: Storage = {
    get length(): number {
      return memory.size;
    },
    clear(): void {
      memory.clear();
    },
    getItem(key: string): string | null {
      return memory.get(key) ?? null;
    },
    key(index: number): string | null {
      return Array.from(memory.keys())[index] ?? null;
    },
    removeItem(key: string): void {
      memory.delete(key);
    },
    setItem(key: string, value: string): void {
      memory.set(key, String(value));
    },
  };
  Object.defineProperty(window, "localStorage", {
    configurable: true,
    writable: false,
    value: polyfill,
  });
}

if (typeof window !== "undefined" && typeof window.matchMedia === "undefined") {
  Object.defineProperty(window, "matchMedia", {
    writable: true,
    value: (query: string): MediaQueryList => ({
      matches: false,
      media: query,
      onchange: null,
      addListener: () => {},
      removeListener: () => {},
      addEventListener: () => {},
      removeEventListener: () => {},
      dispatchEvent: () => false,
    }),
  });
}
