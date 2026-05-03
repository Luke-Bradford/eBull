/**
 * The CSV export contract used to live in this file as ``buildCsv``
 * unit tests. As of Chain 2.8 of #788, the CSV is built server-side
 * from the canonical deduped rollup at
 * ``/instruments/{symbol}/ownership-rollup/export.csv`` and the
 * client-side builder has been removed.
 *
 * The header / row-shape / formula-injection / RFC-4180 contracts
 * are now pinned in ``tests/test_ownership_rollup_csv.py`` against
 * the ``build_rollup_csv`` helper. The L2 ``?view=raw`` link is
 * exercised at the page level by ``OwnershipPage`` integration
 * tests where present.
 *
 * Vitest treats an empty file as a "no test" failure, so we keep
 * one trivial smoke check here as a placeholder until a page-level
 * integration test for the new download link lands.
 */

import { describe, expect, it } from "vitest";

describe("OwnershipPage", () => {
  it("module imports without throwing", async () => {
    // A bare import smoke — picks up syntax errors / missing exports
    // a stricter test would otherwise miss in this slim file.
    const mod = await import("./OwnershipPage");
    expect(typeof mod.OwnershipPage).toBe("function");
  });
});
