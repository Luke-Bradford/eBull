/**
 * /recover page — ADR-0003 Ticket 3 (#116).
 *
 * Surfaces the recovery phrase entry form when the backend reports
 * `recovery_required: true` from /auth/bootstrap-state. Used in two
 * end-to-end flows from the ADR:
 *
 *   1. Edge case A (§5 row 2): encrypted credentials in the DB,
 *      no operators, no key file. The user lands here on first
 *      load, submits a valid phrase, and after the SessionProvider
 *      re-fetches bootstrap-state they are routed onward to /setup.
 *
 *   2. Headline recovery (§5 row 6): an operator restored the DB
 *      onto a fresh machine without `secrets/`. They submit the
 *      phrase from their original install and land back at /login.
 *
 * Discipline (ADR-0003):
 *
 *   - This page only ACCEPTS a phrase. It never displays one. The
 *     two phrase-display surfaces in the product are owned by
 *     Ticket 2 (broker-credentials first save, first-run wizard
 *     first save) and "show me my phrase" is explicitly forbidden
 *     anywhere else.
 *
 *   - The phrase is held only in component state. It is never
 *     written to localStorage / sessionStorage / cookies / IndexedDB,
 *     and it is never passed to console.* — this means error
 *     telemetry must not include the phrase or any derived value.
 *
 *   - Inputs are `autoComplete="off"` and `spellCheck={false}` so
 *     the browser does not autosuggest, autosave, or underline the
 *     wordlist tokens.
 *
 *   - Client-side validation (`verifyPhrase` from
 *     `lib/recoveryPhrase.ts`) runs before any network call. An
 *     unknown word produces a precise "word N is not recognised"
 *     message; a structurally-valid phrase that fails the
 *     SHA-256 checksum produces a "checksum invalid" message. Both
 *     paths short-circuit the request entirely.
 *
 *   - Any 400 response from /auth/recover is treated as the
 *     generic "this phrase doesn't match this installation"
 *     message — the backend deliberately collapses every server-
 *     side failure mode (typo, wrong-but-valid phrase, no row to
 *     verify against) into a single 400 with a fixed detail
 *     string, so the frontend cannot fingerprint them.
 */
import { useEffect, useRef, useState } from "react";
import type {
  ChangeEvent,
  ClipboardEvent,
  FormEvent,
} from "react";
import { useNavigate } from "react-router-dom";

import { ApiError } from "@/api/client";
import { postRecover } from "@/api/auth";
import {
  PHRASE_WORD_COUNT,
  splitPhraseInput,
  verifyPhrase,
  WORDLIST,
} from "@/lib/recoveryPhrase";
import { useSession } from "@/lib/session";

const GENERIC_RECOVER_ERROR =
  "This phrase doesn't match this installation — check you have the right backup.";
const NETWORK_ERROR =
  "Could not reach the server. Check your connection and try again.";
const CONFLICT_ERROR =
  "Recovery is no longer required for this installation.";
const CHECKSUM_ERROR =
  "Recovery phrase checksum is invalid. Check the words and try again.";

function emptyPhrase(): string[] {
  return Array.from({ length: PHRASE_WORD_COUNT }, () => "");
}

export function RecoverPage(): JSX.Element {
  const { status, bootstrapState, refreshBootstrapState } = useSession();
  const navigate = useNavigate();

  const [words, setWords] = useState<string[]>(emptyPhrase);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Track mount + in-flight submit so the post-success
  // refreshBootstrapState() await cannot drive setState after the
  // bounce effect has unmounted us, and so the bounce effect itself
  // does not navigate to /login during the brief window after
  // postRecover resolves but before refreshBootstrapState settles.
  // The latter matters because the post-recover SessionProvider
  // probe runs getMe(), which 401s on a fresh-recover session and
  // would otherwise transition status to "unauthenticated" mid-await.
  const mountedRef = useRef(true);
  const submittingRef = useRef(false);
  useEffect(() => {
    mountedRef.current = true;
    return () => {
      mountedRef.current = false;
    };
  }, []);

  // If the SessionProvider says we are no longer in needs_recovery
  // (e.g. another tab completed recovery, or this tab refreshed
  // bootstrap-state after a successful submit), bounce off this
  // page. The destination is decided by the §6 precedence rule
  // already applied inside SessionProvider — we just navigate to /
  // and RequireAuth / the route guards take it from there.
  useEffect(() => {
    // While a submit is in flight, defer all bounce decisions until
    // refreshBootstrapState() has settled. Otherwise an intermediate
    // SessionProvider probe (e.g. getMe → 401 mid-await) could yank
    // the operator off /recover before the new bootstrap-state is
    // applied, sending them to /login instead of /setup.
    if (submittingRef.current) return;
    if (status === "needs_setup") {
      navigate("/setup", { replace: true });
    } else if (status === "authenticated") {
      navigate("/", { replace: true });
    } else if (status === "unauthenticated") {
      navigate("/login", { replace: true });
    }
  }, [status, navigate]);

  function handleWordChange(index: number, value: string): void {
    setWords((prev) => {
      const next = [...prev];
      next[index] = value;
      return next;
    });
    if (error !== null) setError(null);
  }

  /**
   * Paste handler attached to every input. If the clipboard payload
   * tokenises into more than one word, fan it out across the inputs
   * starting at the index that received the paste. This lets the
   * operator paste a full 24-word phrase into any single field
   * (the most ergonomic shape for clipboards that strip newlines).
   */
  function handlePaste(
    index: number,
    event: ClipboardEvent<HTMLInputElement>,
  ): void {
    const text = event.clipboardData.getData("text");
    const tokens = splitPhraseInput(text);
    if (tokens.length <= 1) {
      // Single token: let the default paste behaviour fill the
      // single input. No fan-out needed.
      return;
    }
    event.preventDefault();
    setWords((prev) => {
      const next = [...prev];
      for (let i = 0; i < tokens.length && index + i < PHRASE_WORD_COUNT; i++) {
        next[index + i] = tokens[i]!;
      }
      return next;
    });
    if (error !== null) setError(null);
  }

  function safeSetError(message: string | null): void {
    if (mountedRef.current) setError(message);
  }

  async function handleSubmit(event: FormEvent<HTMLFormElement>): Promise<void> {
    event.preventDefault();
    setError(null);

    const trimmed = words.map((w) => w.trim().toLowerCase());

    // 1. Length / non-empty check (cheap, no crypto).
    if (trimmed.some((w) => w === "")) {
      setError(`Enter all ${PHRASE_WORD_COUNT} words.`);
      return;
    }

    // Flip the in-flight gate BEFORE awaiting verifyPhrase so that
    // a SubtleCrypto failure (e.g. insecure context) lands in the
    // single outer try/catch below, and so the bounce effect treats
    // any intermediate session probe as a no-op.
    submittingRef.current = true;
    setSubmitting(true);
    try {
      // 2. Client-side full validation (unknown word + checksum).
      const result = await verifyPhrase(trimmed);
      if (!result.ok) {
        switch (result.error.kind) {
          case "wrong_length":
            safeSetError(`Enter all ${PHRASE_WORD_COUNT} words.`);
            return;
          case "unknown_word":
            safeSetError(`Word ${result.error.position} is not recognised.`);
            return;
          case "bad_checksum":
            safeSetError(CHECKSUM_ERROR);
            return;
        }
      }

      // 3. Submit and re-apply the §6 precedence rule.
      await postRecover(trimmed.join(" "));
      await refreshBootstrapState();
    } catch (err: unknown) {
      if (err instanceof ApiError) {
        if (err.status === 409) {
          safeSetError(CONFLICT_ERROR);
          // Re-sync state so the bounce effect can fire (after the
          // submittingRef flag is cleared in the finally below).
          void refreshBootstrapState();
        } else if (err.status === 400) {
          safeSetError(GENERIC_RECOVER_ERROR);
        } else {
          safeSetError(NETWORK_ERROR);
        }
      } else {
        // Non-ApiError throws (SubtleCrypto, network DNS, etc.)
        // surface as the same generic network message rather than
        // an unhandled rejection.
        safeSetError(NETWORK_ERROR);
      }
    } finally {
      submittingRef.current = false;
      // Guarded so the post-success unmount path does not warn.
      if (mountedRef.current) setSubmitting(false);
    }
  }

  // If the operator hits /recover directly while no recovery is in
  // progress (status is still "loading" before the first probe
  // completes, or already past it), render a minimal placeholder
  // rather than the form. The bounce effect above handles the
  // post-load case; this just covers the brief loading window.
  if (
    status === "loading" ||
    (bootstrapState !== null && !bootstrapState.recovery_required)
  ) {
    return (
      <div className="flex h-screen w-screen items-center justify-center text-sm text-slate-400">
        Loading…
      </div>
    );
  }

  return (
    <div className="flex h-screen w-screen items-center justify-center bg-slate-50">
      <form
        onSubmit={handleSubmit}
        className="w-full max-w-2xl rounded border border-slate-200 bg-white p-6 shadow-sm"
      >
        <h1 className="mb-1 text-lg font-semibold text-slate-800">
          Recover existing eBull data
        </h1>
        <p className="mb-4 text-xs text-slate-500">
          This installation has encrypted broker credentials in the
          database but no local key. Enter the 24-word recovery phrase
          you wrote down when you first added a credential. eBull
          checks the phrase locally before sending it. The phrase is
          never stored in your browser.
        </p>
        <ol
          aria-label="Recovery phrase inputs"
          className="mb-4 grid grid-cols-2 gap-2 sm:grid-cols-3"
        >
          {words.map((word, index) => {
            const inputId = `recover-word-${index}`;
            return (
              <li key={index} className="flex items-center gap-2 text-xs">
                <label
                  htmlFor={inputId}
                  className="w-6 text-right tabular-nums text-slate-400"
                >
                  {index + 1}.
                </label>
                <input
                  id={inputId}
                  type="text"
                  // Use the BIP39 wordlist as a same-page datalist
                  // for autocomplete. This keeps the wordlist
                  // contained to the page and avoids any "guess
                  // my next word from history" behaviour from the
                  // browser's autofill engine.
                  list="recover-wordlist"
                  autoComplete="off"
                  spellCheck={false}
                  autoCapitalize="none"
                  value={word}
                  // The visible <label> text is the position number
                  // ("1.") only; aria-label supplies the full
                  // "Word N" accessible name for screen readers and
                  // for *ByLabelText queries in tests.
                  aria-label={`Word ${index + 1}`}
                  onChange={(e: ChangeEvent<HTMLInputElement>) =>
                    handleWordChange(index, e.target.value)
                  }
                  onPaste={(e) => handlePaste(index, e)}
                  className="w-full rounded border border-slate-300 px-2 py-1 font-mono text-sm text-slate-800"
                />
              </li>
            );
          })}
        </ol>
        <datalist id="recover-wordlist">
          {WORDLIST.map((w) => (
            <option key={w} value={w} />
          ))}
        </datalist>
        {error !== null && (
          <div
            role="alert"
            className="mb-3 rounded bg-rose-50 px-2 py-1.5 text-xs text-rose-700"
          >
            {error}
          </div>
        )}
        <button
          type="submit"
          disabled={submitting}
          className="w-full rounded bg-slate-800 py-2 text-sm font-medium text-white disabled:bg-slate-400"
        >
          {submitting ? "Recovering…" : "Recover"}
        </button>
      </form>
    </div>
  );
}
