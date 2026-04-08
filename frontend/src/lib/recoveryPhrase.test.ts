/**
 * Tests for recoveryPhrase.ts (#116 / ADR-0003 Ticket 3).
 *
 * The "all zeros" 32-byte seed is the canonical BIP39 test vector;
 * its valid 24-word recovery phrase is 23 × "abandon" + "art". This
 * pins the encoding parity with the backend (`recovery_phrase.py`)
 * without depending on a live backend in unit tests.
 */
import { describe, expect, it } from "vitest";

import {
  PHRASE_WORD_COUNT,
  isWordInList,
  splitPhraseInput,
  verifyPhrase,
  WORDLIST,
} from "./recoveryPhrase";

const VALID_PHRASE: readonly string[] = [
  ...Array(23).fill("abandon"),
  "art",
];

// Non-trivial parity vector. Exercises 32 distinct entropy byte
// values (`bytes(range(32))`) so any byte-order regression in the
// TS port — which the all-zeros vector cannot detect because every
// byte is identical — surfaces against the backend immediately.
//
// To re-derive this string from the source of truth (the backend
// encoder in `app/security/recovery_phrase.py`), run:
//
//   uv run python -c "from app.security.recovery_phrase import encode_phrase; print(' '.join(encode_phrase(bytes(range(32)))))"
//
// Output is the exact string below. If this test ever starts
// failing after a backend change to the encoder, re-run the
// command above and replace the literal — DO NOT hand-edit it.
const NON_TRIVIAL_PHRASE: readonly string[] =
  "abandon amount liar amount expire adjust cage candy arch gather drum bullet absurd math era live bid rhythm alien crouch range attend journey unaware".split(
    " ",
  );

describe("recoveryPhrase", () => {
  it("vendors exactly 2048 wordlist entries", () => {
    expect(WORDLIST).toHaveLength(2048);
    expect(WORDLIST[0]).toBe("abandon");
    expect(WORDLIST[2047]).toBe("zoo");
  });

  it("PHRASE_WORD_COUNT is 24", () => {
    expect(PHRASE_WORD_COUNT).toBe(24);
  });

  it("isWordInList recognises wordlist entries case-insensitively", () => {
    expect(isWordInList("abandon")).toBe(true);
    expect(isWordInList("  ART  ")).toBe(true);
    expect(isWordInList("notaword")).toBe(false);
  });

  describe("splitPhraseInput", () => {
    it("splits on any whitespace", () => {
      expect(splitPhraseInput("one two\tthree\nfour  five")).toEqual([
        "one",
        "two",
        "three",
        "four",
        "five",
      ]);
    });

    it("lowercases and trims", () => {
      expect(splitPhraseInput("  Abandon  ART  ")).toEqual(["abandon", "art"]);
    });

    it("drops empty tokens", () => {
      expect(splitPhraseInput("   ")).toEqual([]);
    });
  });

  describe("verifyPhrase", () => {
    it("accepts the canonical all-zeros test vector", async () => {
      const result = await verifyPhrase(VALID_PHRASE);
      expect(result.ok).toBe(true);
    });

    it("accepts a non-trivial entropy vector that exercises every byte position", async () => {
      // Pins byte-order parity with the backend port. Generated
      // by `encode_phrase(bytes(range(32)))` in
      // app/security/recovery_phrase.py.
      const result = await verifyPhrase(NON_TRIVIAL_PHRASE);
      expect(result.ok).toBe(true);
    });

    it("rejects wrong length", async () => {
      const result = await verifyPhrase(["abandon", "abandon"]);
      expect(result).toEqual({
        ok: false,
        error: { kind: "wrong_length", got: 2 },
      });
    });

    it("rejects an unknown word with the 1-based position and the offending token", async () => {
      const phrase = [...VALID_PHRASE];
      phrase[4] = "notaword";
      const result = await verifyPhrase(phrase);
      expect(result).toEqual({
        ok: false,
        error: { kind: "unknown_word", position: 5, word: "notaword" },
      });
    });

    it("rejects a phrase with the wrong checksum", async () => {
      // Replace the final word ("art") with a different valid wordlist
      // entry. The checksum byte will no longer match SHA256(entropy)[0],
      // so the structural decode succeeds but checksum verification fails.
      const phrase = [...VALID_PHRASE];
      phrase[23] = "ability";
      const result = await verifyPhrase(phrase);
      expect(result).toEqual({ ok: false, error: { kind: "bad_checksum" } });
    });

    it("is case-insensitive and tolerates surrounding whitespace", async () => {
      const phrase = VALID_PHRASE.map((w, i) =>
        i === 0 ? "  Abandon " : i === 23 ? "ART" : w,
      );
      const result = await verifyPhrase(phrase);
      expect(result.ok).toBe(true);
    });
  });
});
