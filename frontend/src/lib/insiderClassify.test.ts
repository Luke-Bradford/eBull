import { describe, expect, it } from "vitest";

import {
  directionOf,
  notionalValue,
  signedShares,
} from "@/lib/insiderClassify";

describe("insiderClassify", () => {
  describe("directionOf", () => {
    it("prefers explicit acquired_disposed_code over txn_code", () => {
      // S would normally classify disposed; explicit A flag overrides.
      expect(directionOf("A", "S")).toBe("acquired");
      expect(directionOf("D", "P")).toBe("disposed");
    });

    it("falls back to txn_code when acquired_disposed_code is null", () => {
      expect(directionOf(null, "P")).toBe("acquired");
      expect(directionOf(null, "S")).toBe("disposed");
      expect(directionOf(null, "M")).toBe("acquired"); // option exercise
      expect(directionOf(null, "F")).toBe("disposed"); // tax withholding
      expect(directionOf(null, "G")).toBe("disposed"); // gift
    });

    it("returns unknown when neither signal is recognised", () => {
      expect(directionOf(null, "Z")).toBe("unknown");
      expect(directionOf("X", "Z")).toBe("unknown"); // X on AD code is invalid
    });
  });

  describe("signedShares", () => {
    it("returns positive for acquired, negative for disposed, zero otherwise", () => {
      expect(signedShares("100", "A", "P")).toBe(100);
      expect(signedShares("100", "D", "S")).toBe(-100);
      expect(signedShares("100", null, "Z")).toBe(0);
    });

    it("returns zero for null or non-finite shares", () => {
      expect(signedShares(null, "A", "P")).toBe(0);
      expect(signedShares("not-a-number", "A", "P")).toBe(0);
    });
  });

  describe("notionalValue", () => {
    it("multiplies shares by price when both present", () => {
      expect(notionalValue("100", "12.5")).toBe(1250);
    });

    it("returns zero when either is null or invalid", () => {
      expect(notionalValue(null, "12.5")).toBe(0);
      expect(notionalValue("100", null)).toBe(0);
      expect(notionalValue("100", "not-a-number")).toBe(0);
    });
  });
});
