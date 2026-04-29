import { describe, expect, it } from "vitest";

import {
  filingTypeFriendlyName,
  GLOSSARY,
  lookupTerm,
} from "@/lib/glossary";

describe("GLOSSARY", () => {
  it("contains no duplicate terms", () => {
    const seen = new Set<string>();
    for (const entry of GLOSSARY) {
      expect(seen.has(entry.term), `duplicate term: ${entry.term}`).toBe(false);
      seen.add(entry.term);
    }
  });

  it("has a non-empty shortName + what + why for every entry", () => {
    for (const entry of GLOSSARY) {
      expect(entry.shortName.length, entry.term).toBeGreaterThan(0);
      expect(entry.what.length, entry.term).toBeGreaterThan(0);
      expect(entry.why.length, entry.term).toBeGreaterThan(0);
    }
  });

  it("keeps shortName tight (≤ 32 chars) so tooltips don't bloat", () => {
    for (const entry of GLOSSARY) {
      expect(
        entry.shortName.length,
        `${entry.term}: shortName "${entry.shortName}" too long`,
      ).toBeLessThanOrEqual(32);
    }
  });
});

describe("lookupTerm", () => {
  it("returns the entry for a known term", () => {
    const entry = lookupTerm("CIK");
    expect(entry).not.toBeNull();
    expect(entry?.shortName).toBe("SEC entity ID");
  });

  it("returns null for an unknown term", () => {
    expect(lookupTerm("NEVER_HEARD_OF_IT")).toBeNull();
  });

  it("is case-sensitive (terms render exactly as the operator sees them)", () => {
    expect(lookupTerm("cik")).toBeNull();
    expect(lookupTerm("CIK")).not.toBeNull();
  });
});

describe("filingTypeFriendlyName", () => {
  it("returns the glossary shortName for known form types", () => {
    expect(filingTypeFriendlyName("8-K")).toBe("Material event");
    expect(filingTypeFriendlyName("10-K")).toBe("Annual report");
    expect(filingTypeFriendlyName("10-Q")).toBe("Quarterly report");
    expect(filingTypeFriendlyName("8-K/A")).toBe("Material event amendment");
  });

  it("falls back to the raw type when the glossary doesn't recognise it", () => {
    // Exotic form — keep it visible rather than swallowing into "filing".
    expect(filingTypeFriendlyName("NT 10-K")).toBe("NT 10-K");
  });

  it("returns 'filing' for null", () => {
    expect(filingTypeFriendlyName(null)).toBe("filing");
  });
});
