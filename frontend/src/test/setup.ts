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
import { afterEach } from "vitest";
import { cleanup } from "@testing-library/react";
import "@testing-library/jest-dom/vitest";

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
