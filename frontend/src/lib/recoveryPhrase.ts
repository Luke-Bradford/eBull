/**
 * Recovery phrase decode/validate utilities (frontend port of
 * `app/security/recovery_phrase.py`, ADR-0003 / #114).
 *
 * Pure functions, no I/O at call time. The 2048-word BIP39 English
 * wordlist is vendored at `bip39-wordlist.txt` and imported as raw
 * text via Vite's `?raw` loader so this module works offline and
 * needs no build step beyond the standard Vite pipeline.
 *
 * Terminology discipline (ADR-0003): the user-facing concept is
 * "recovery phrase". This file uses BIP39 internally only because
 * that is the upstream wordlist's actual name; nothing exported
 * here surfaces "BIP39", "mnemonic", or "seed" to the operator.
 *
 * The frontend uses these helpers to:
 *   - validate a 24-word phrase BEFORE submitting to /auth/recover
 *     so a typo surfaces as a precise "word N is not recognised"
 *     message rather than the backend's generic 400
 *   - confirm the checksum byte locally so a phrase with the right
 *     shape but a transcription error fails fast
 *
 * What this file deliberately does NOT do:
 *   - encode a phrase from entropy (the frontend never holds the
 *     root secret; encoding belongs to the backend)
 *   - any persistence — the phrase is held only in component state
 *     and is never written to localStorage / sessionStorage / cookies
 *   - any logging — callers must not pass the phrase to console.*
 */
import wordlistRaw from "./bip39-wordlist.txt?raw";

export const PHRASE_WORD_COUNT = 24;
export const ROOT_SECRET_BYTES = 32;
const BITS_PER_WORD = 11;
const WORDLIST_LEN = 2048;

export const WORDLIST: readonly string[] = (() => {
  const words = wordlistRaw
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line !== "");
  if (words.length !== WORDLIST_LEN) {
    throw new Error(
      `recoveryPhrase wordlist must be exactly ${WORDLIST_LEN} entries (got ${words.length})`,
    );
  }
  return words;
})();

const WORD_INDEX: ReadonlyMap<string, number> = new Map(
  WORDLIST.map((word, idx) => [word, idx] as const),
);

export type DecodeError =
  | { kind: "wrong_length"; got: number }
  | { kind: "unknown_word"; position: number; word: string }
  | { kind: "bad_checksum" };

export interface DecodeSuccess {
  ok: true;
}

export interface DecodeFailure {
  ok: false;
  error: DecodeError;
}

export type DecodeResult = DecodeSuccess | DecodeFailure;

/**
 * Split a free-form phrase string into trimmed lowercase tokens.
 * Used by the paste handler so an operator can paste 24 words
 * separated by spaces, tabs, or newlines.
 */
export function splitPhraseInput(raw: string): string[] {
  return raw
    .split(/\s+/)
    .map((word) => word.trim().toLowerCase())
    .filter((word) => word !== "");
}

export function isWordInList(word: string): boolean {
  return WORD_INDEX.has(word.trim().toLowerCase());
}

/**
 * Full validation: 24 words, all in the wordlist, checksum byte
 * matches `SHA256(rootSecret)[0]`. The result is a discriminated
 * union rather than an exception so the page can render specific
 * messages ("word N is not recognised", "checksum invalid")
 * without try/catch noise; the variants mirror the backend
 * `RecoveryPhraseError` messages for parity.
 *
 * Async because SubtleCrypto.digest is async. The page awaits
 * this once on submit (not on every keystroke). The derived
 * 32-byte root secret is dropped before this function returns —
 * the frontend never holds it; the backend re-derives it from
 * the phrase string when /auth/recover runs.
 */
export async function verifyPhrase(
  words: readonly string[],
): Promise<DecodeResult> {
  if (words.length !== PHRASE_WORD_COUNT) {
    return { ok: false, error: { kind: "wrong_length", got: words.length } };
  }

  // Re-pack to recover both entropy and the embedded checksum byte.
  let bits = 0n;
  for (let position = 0; position < PHRASE_WORD_COUNT; position++) {
    const word = words[position]!.trim().toLowerCase();
    const idx = WORD_INDEX.get(word);
    if (idx === undefined) {
      return {
        ok: false,
        error: { kind: "unknown_word", position: position + 1, word },
      };
    }
    bits = (bits << BigInt(BITS_PER_WORD)) | BigInt(idx);
  }
  const embeddedChecksum = Number(bits & 0xffn);
  let entropyInt = bits >> 8n;

  const rootSecret = new Uint8Array(ROOT_SECRET_BYTES);
  for (let i = ROOT_SECRET_BYTES - 1; i >= 0; i--) {
    rootSecret[i] = Number(entropyInt & 0xffn);
    entropyInt >>= 8n;
  }

  const digest = new Uint8Array(
    await crypto.subtle.digest("SHA-256", rootSecret),
  );
  // Zero the local copy regardless of outcome — there is no need
  // for the root secret to outlive this function.
  try {
    if (digest[0] !== embeddedChecksum) {
      return { ok: false, error: { kind: "bad_checksum" } };
    }
    return { ok: true };
  } finally {
    rootSecret.fill(0);
  }
}
