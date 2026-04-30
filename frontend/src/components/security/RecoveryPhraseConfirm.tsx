/**
 * RecoveryPhraseConfirm — ADR-0003 Ticket 2a (#120).
 *
 * Reusable inner content for the recovery-phrase confirmation flow. Used
 * by the broker-credentials first-save modal (#121 / 2b) and by the
 * first-run wizard's inline first-save path (#122 / 2c). The phrase is
 * displayed exactly twice in the product's lifetime — at one of the two
 * lazy-generation moments — and never again. There is no "show me my
 * phrase" surface anywhere else in the app, and there must not be: ADR
 * 0003 §"Rejected alternatives" rules it out explicitly.
 *
 * What this component owns
 *   - 24-word phrase display in a 2-column numbered list
 *   - "I have written this down" gate (cannot proceed until checked)
 *   - 3 randomly chosen word-position challenges
 *   - case-insensitive trimmed comparison against the supplied phrase
 *   - confirm/cancel callbacks
 *   - fail-closed render when the supplied phrase is not exactly 24 words
 *
 * What this component does NOT own
 *   - the modal shell, overlay, focus trap, or escape-key handling
 *     (these belong to 2b / 2c — see ADR-0003 §3 and the parent ticket
 *     #115 split discussion)
 *   - any network call — the parent supplies the phrase from the
 *     `recovery_phrase` field on the POST /broker-credentials response
 *     (see app/api/broker_credentials.py)
 *   - any persistence — the phrase is never written to localStorage,
 *     sessionStorage, IndexedDB, cookies, or anywhere else; it is held
 *     in component state only and discarded on unmount
 *   - any logging — the phrase is never passed to console.* anywhere
 *
 * Terminology discipline (ADR-0003 §"Consequences"): user-facing copy
 * uses "recovery phrase" only. Never "seed phrase", "mnemonic",
 * "wallet", "BIP39", or "backup phrase". This applies to visible copy,
 * aria labels, tooltips, and test names.
 */
import { useState } from "react";
import type { ChangeEvent, FormEvent } from "react";

const REQUIRED_PHRASE_LENGTH = 24;
const DEFAULT_CHALLENGE_COUNT = 3;
const UNAVAILABLE_MESSAGE =
  "Recovery phrase unavailable. Cancel and try again.";

export interface RecoveryPhraseConfirmProps {
  /**
   * The 24-word recovery phrase, supplied by the parent. The component
   * does not fetch, persist, or log this value. Must contain exactly
   * `REQUIRED_PHRASE_LENGTH` non-empty words; any other shape renders
   * the fail-closed unavailable state.
   */
  readonly phrase: readonly string[];
  /** Called once the operator has correctly answered every challenge. */
  readonly onConfirmed: () => void;
  /** Called when the operator dismisses the flow without confirming. */
  readonly onCancel: () => void;
  /**
   * Test seam only. Production callers must not set this — the
   * 3-word challenge count is fixed product behaviour per ADR-0003 §3.
   * Exposed so unit tests can drive deterministic challenges without
   * monkey-patching crypto.getRandomValues.
   */
  readonly challengeCount?: number;
}

interface ChallengeState {
  /** Indices into `phrase` for each challenge slot, in display order. */
  readonly indices: readonly number[];
  /** Current input value for each challenge slot, indexed by slot. */
  readonly entries: readonly string[];
}

/**
 * Draw a single uniformly-distributed integer in `[0, length)` from
 * `crypto.getRandomValues` using rejection sampling.
 *
 * A naive `value % length` is biased whenever `2^32` is not an exact
 * multiple of `length` — values in `[0, 2^32 mod length)` map to
 * lower bins more often than higher bins. With `length = 24` the bias
 * per draw is ~1.5e-8, which is not a security boundary in itself, but
 * the component is deliberately using a CSPRNG to head off "predictable
 * RNG inside a security component" review findings, and biased
 * sampling defeats that argument by the same logic. We discard any
 * draw that lands in the residue zone above the largest multiple of
 * `length` that fits in `2^32` and re-roll.
 */
function uniformIndexBelow(length: number): number {
  const buf = new Uint32Array(1);
  // 2^32 - (2^32 mod length) = largest multiple of length <= 2^32.
  // Computed in BigInt to avoid the 2^32 overflow.
  const limit = Number(2n ** 32n - (2n ** 32n % BigInt(length)));
  for (;;) {
    crypto.getRandomValues(buf);
    if (buf[0]! < limit) {
      return buf[0]! % length;
    }
  }
}

/**
 * Pick `count` distinct indices in `[0, length)` uniformly.
 * `Math.random` is deliberately avoided — see `uniformIndexBelow`.
 */
function pickChallengeIndices(length: number, count: number): readonly number[] {
  const safeCount = Math.min(count, length);
  const chosen = new Set<number>();
  while (chosen.size < safeCount) {
    chosen.add(uniformIndexBelow(length));
  }
  return [...chosen].sort((a, b) => a - b);
}

function buildInitialChallenge(
  phrase: readonly string[],
  challengeCount: number,
  phraseValid: boolean,
): ChallengeState {
  if (!phraseValid) {
    return { indices: [], entries: [] };
  }
  const indices = pickChallengeIndices(phrase.length, challengeCount);
  return { indices, entries: indices.map(() => "") };
}

/**
 * Stable content signature for a phrase array. Used to detect when the
 * parent has actually swapped one phrase for another (vs. just passing
 * a new array reference with the same content on every render). The
 * NUL separator is safe because the wordlist is ASCII.
 */
function phraseContentSignature(phrase: readonly string[]): string {
  return phrase.join("\0");
}

function isPhraseShapeValid(phrase: readonly string[]): boolean {
  if (phrase.length !== REQUIRED_PHRASE_LENGTH) {
    return false;
  }
  return phrase.every((word) => typeof word === "string" && word.trim() !== "");
}

function normaliseWord(word: string): string {
  return word.trim().toLowerCase();
}

export function RecoveryPhraseConfirm({
  phrase,
  onConfirmed,
  onCancel,
  challengeCount = DEFAULT_CHALLENGE_COUNT,
}: RecoveryPhraseConfirmProps): JSX.Element {
  // Fail closed on bad props. We do not try to "best-effort" render a
  // partial phrase — the operator must back out and the parent must
  // re-fetch a fresh full phrase. Rendering anything from a malformed
  // phrase risks confirming the wrong material.
  const phraseValid = isPhraseShapeValid(phrase);

  // Stage 1: display + "I have written this down" gate.
  // Stage 2: challenge inputs.
  // The two stages live in one component so the parent only ever sees
  // confirm/cancel — the gate is not a separate caller-visible state.
  const [stage, setStage] = useState<"display" | "challenge">("display");
  const [writtenDown, setWrittenDown] = useState(false);
  const [showError, setShowError] = useState(false);

  // Challenge indices are computed once on mount via the initializer.
  // For subsequent renders we detect a real phrase content change
  // (signature comparison, not reference) and regenerate. The
  // signature dance is the React docs' "storing information from
  // previous renders" pattern: compare-and-set during render so the
  // reset is visible in the same commit, no useEffect cycle gap. This
  // matters because:
  //   - if the parent mounts the component with an invalid phrase
  //     (e.g. before the API response arrives) then swaps to a valid
  //     one, the initializer's [] is stale and we must recompute;
  //   - if the parent passes a fresh phrase array reference every
  //     render with the same content, we must NOT regenerate (the
  //     "stable across rerender" regression test pins this).
  const currentSignature = phraseContentSignature(phrase);
  const [previousSignature, setPreviousSignature] = useState(currentSignature);
  const [challenge, setChallenge] = useState<ChallengeState>(() =>
    buildInitialChallenge(phrase, challengeCount, phraseValid),
  );

  if (previousSignature !== currentSignature) {
    setPreviousSignature(currentSignature);
    setChallenge(buildInitialChallenge(phrase, challengeCount, phraseValid));
    setStage("display");
    setWrittenDown(false);
    setShowError(false);
  }

  function isAllCorrect(state: ChallengeState): boolean {
    if (!phraseValid || state.indices.length === 0) {
      return false;
    }
    return state.indices.every((wordIndex, slot) => {
      const expected = normaliseWord(phrase[wordIndex]!);
      const entered = normaliseWord(state.entries[slot] ?? "");
      return entered !== "" && entered === expected;
    });
  }

  if (!phraseValid) {
    return (
      <div
        role="alert"
        aria-live="assertive"
        className="flex flex-col gap-4 rounded border border-red-200 bg-red-50 p-4 text-sm text-red-700"
      >
        <p className="font-semibold">{UNAVAILABLE_MESSAGE}</p>
        <div className="flex justify-end">
          <button
            type="button"
            onClick={onCancel}
            className="rounded border border-red-300 bg-white dark:bg-slate-900 px-3 py-1.5 text-xs font-medium text-red-700 hover:bg-red-100"
          >
            Cancel
          </button>
        </div>
      </div>
    );
  }

  function handleEntryChange(slot: number, value: string): void {
    setChallenge((prev) => {
      const next = [...prev.entries];
      next[slot] = value;
      return { indices: prev.indices, entries: next };
    });
    if (showError) {
      setShowError(false);
    }
  }

  function handleSubmit(event: FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    if (isAllCorrect(challenge)) {
      onConfirmed();
    } else {
      setShowError(true);
    }
  }

  function advanceToChallenge(): void {
    if (writtenDown) {
      setStage("challenge");
    }
  }

  if (stage === "display") {
    return (
      <section
        aria-labelledby="recovery-phrase-display-heading"
        className="flex flex-col gap-4"
      >
        <header>
          <h2
            id="recovery-phrase-display-heading"
            className="text-sm font-semibold text-slate-700"
          >
            Write down your recovery phrase
          </h2>
          <p className="mt-1 text-xs text-slate-500">
            These 24 words are the only way to recover your encrypted broker
            credentials if you lose this machine. eBull will not show them
            again. Write them down on paper and store them somewhere safe.
          </p>
        </header>
        <ol
          aria-label="Recovery phrase words"
          className="grid grid-cols-2 gap-x-6 gap-y-2 rounded border border-slate-200 dark:border-slate-800 bg-slate-50 dark:bg-slate-900/40 p-4 font-mono text-sm text-slate-800 dark:text-slate-100"
        >
          {phrase.map((word, index) => (
            <li
              // The phrase length is fixed at 24 and the order is the
              // payload, so the index is a stable key here.
              key={index}
              className="flex items-baseline gap-2 tabular-nums"
            >
              <span
                aria-hidden="true"
                className="w-6 text-right text-xs text-slate-400"
              >
                {index + 1}.
              </span>
              <span>
                <span className="sr-only">{`Word ${index + 1}: `}</span>
                {word}
              </span>
            </li>
          ))}
        </ol>
        <label className="flex items-start gap-2 text-xs text-slate-700">
          <input
            type="checkbox"
            checked={writtenDown}
            onChange={(event: ChangeEvent<HTMLInputElement>) =>
              setWrittenDown(event.target.checked)
            }
            className="mt-0.5"
          />
          <span>
            I have written down all 24 words and stored them somewhere safe.
          </span>
        </label>
        <div className="flex justify-end gap-2">
          <button
            type="button"
            onClick={onCancel}
            className="rounded border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:hover:bg-slate-800"
          >
            Cancel
          </button>
          <button
            type="button"
            onClick={advanceToChallenge}
            disabled={!writtenDown}
            className="rounded bg-slate-800 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-700 disabled:cursor-not-allowed disabled:bg-slate-400"
          >
            Continue
          </button>
        </div>
      </section>
    );
  }

  return (
    <form
      aria-labelledby="recovery-phrase-confirm-heading"
      onSubmit={handleSubmit}
      className="flex flex-col gap-4"
    >
      <header>
        <h2
          id="recovery-phrase-confirm-heading"
          className="text-sm font-semibold text-slate-700"
        >
          Confirm your recovery phrase
        </h2>
        <p className="mt-1 text-xs text-slate-500">
          Type the words at the positions shown to confirm you have written
          them down correctly.
        </p>
      </header>
      <div className="flex flex-col gap-3">
        {challenge.indices.map((wordIndex, slot) => {
          const inputId = `recovery-phrase-challenge-${slot}`;
          return (
            <label
              key={wordIndex}
              htmlFor={inputId}
              className="flex flex-col gap-1 text-xs text-slate-700"
            >
              <span>{`Word #${wordIndex + 1}`}</span>
              <input
                id={inputId}
                type="text"
                autoComplete="off"
                spellCheck={false}
                value={challenge.entries[slot] ?? ""}
                onChange={(event: ChangeEvent<HTMLInputElement>) =>
                  handleEntryChange(slot, event.target.value)
                }
                className="rounded border border-slate-300 px-2 py-1.5 font-mono text-sm text-slate-800 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100"
              />
            </label>
          );
        })}
      </div>
      {showError ? (
        <div
          role="alert"
          className="rounded bg-red-50 px-2 py-1.5 text-xs text-red-700"
        >
          Those words do not match your recovery phrase. Check the position
          numbers and try again.
        </div>
      ) : null}
      <div className="flex justify-end gap-2">
        <button
          type="button"
          onClick={onCancel}
          className="rounded border border-slate-300 bg-white px-3 py-1.5 text-xs font-medium text-slate-700 hover:bg-slate-100 dark:border-slate-700 dark:bg-slate-900 dark:text-slate-100 dark:hover:bg-slate-800"
        >
          Cancel
        </button>
        {/* Submit is always enabled — disabled submit buttons are not
            announced by assistive tech, and the validation message
            below is the operator-visible failure mode anyway. */}
        <button
          type="submit"
          className="rounded bg-slate-800 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-700"
        >
          Confirm
        </button>
      </div>
    </form>
  );
}
