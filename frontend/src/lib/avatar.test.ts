import { describe, expect, it } from "vitest";
import { avatarTone } from "@/lib/avatar";

const TONES = [
  "bg-blue-600",
  "bg-emerald-600",
  "bg-amber-600",
  "bg-rose-600",
  "bg-violet-600",
  "bg-cyan-600",
];

describe("avatarTone", () => {
  it("is deterministic — same name yields the same tone", () => {
    expect(avatarTone("@gurutrader")).toBe(avatarTone("@gurutrader"));
  });

  it("always returns a known tone class", () => {
    for (const name of ["", "a", "@gurutrader", "Zoë", "12345", "  spaced  "]) {
      expect(TONES).toContain(avatarTone(name));
    }
  });

  it("distributes distinct names across more than one tone", () => {
    const names = Array.from({ length: 40 }, (_, i) => `trader-${i}`);
    const distinct = new Set(names.map(avatarTone));
    expect(distinct.size).toBeGreaterThan(1);
  });

  it("empty string is handled without throwing", () => {
    expect(() => avatarTone("")).not.toThrow();
    expect(TONES).toContain(avatarTone(""));
  });
});
