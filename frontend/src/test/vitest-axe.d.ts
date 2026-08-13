/**
 * Type-augmentation shim for vitest-axe 0.1.0 against vitest 2.x (#1086).
 *
 * vitest-axe 0.1.0 augments the legacy `Vi.Assertion` namespace, which
 * vitest 2.x removed in favour of `@vitest/expect` exporting the
 * `Assertion` interface directly. Without this shim, calls to
 * `expect(...).toHaveNoViolations()` type-check fail even though they
 * run correctly. Augmenting the live module here re-exposes the matcher
 * to the type system without forking vitest-axe upstream.
 */
// `export {}` keeps this file a module so `declare module "vitest"`
// below is parsed as augmentation rather than a top-level module
// declaration that replaces vitest's own exports. Without it, `import
// { it, vi, expect } from "vitest"` stops resolving repo-wide.
export {};

interface AxeMatchers<R = unknown> {
  toHaveNoViolations(): R;
}

declare module "vitest" {
  interface Assertion<T = unknown> extends AxeMatchers {}
  interface AsymmetricMatchersContaining extends AxeMatchers {}
}

declare module "@vitest/expect" {
  interface Assertion<T = unknown> extends AxeMatchers {}
  interface AsymmetricMatchersContaining extends AxeMatchers {}
}
