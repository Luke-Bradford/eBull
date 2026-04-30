/**
 * RecoveryPhraseModal — shared phrase-confirmation modal wrapper
 * (extracted from #121's SettingsPage during #122 / ADR-0003 Ticket 2c).
 *
 * Composes:
 *   - <Modal>            — shell, focus trap, escape handling, no
 *                          overlay-click dismissal (#121)
 *   - <RecoveryPhraseConfirm>
 *                        — 24-word display + 3-word challenge (#120)
 *   - confirm-cancel inner view (#121)
 *
 * Why this exists:
 *   ADR-0003 Ticket 2 has two callers — the broker-credentials section
 *   on the Settings page (#121) and the optional broker step in the
 *   first-run wizard (#122). Both must run *the same* flow:
 *
 *     - same modal shell, same focus trap, same escape semantics
 *     - same fail-closed cancel gate with the same warning copy
 *     - same "phrase lives only in component state" guarantee
 *
 * Duplicating that across two pages would diverge over time and was
 * the explicit motivation for the split-into-2a-2b-2c work. This
 * component is the seam that keeps both call sites identical.
 *
 * What this owns:
 *   - the `phrase` state and the `phraseModalView` discriminated state
 *     ("confirm" | "confirm-cancel")
 *   - the imperative `open(phrase)` handle exposed via `useRecoveryPhraseModal`
 *   - the close path: clear phrase, reset view, fire `onClose`
 *   - the confirm-cancel JSX with the installation-wide warning copy
 *
 * What this does NOT own:
 *   - any post-close action (refresh / navigate / clear-create-error).
 *     The hook caller passes an `onClose` callback that runs from a
 *     SINGLE close path (challenge confirm AND "Close anyway" both
 *     route through it). This was the round-2 review fix on #121
 *     (review feedback "stale createError after modal close") and is
 *     pinned by tests in both call sites.
 *
 * Security model (carried forward from #120 + #121):
 *   - the phrase lives ONLY in the hook's state for the lifetime of
 *     the modal. Never written to localStorage, sessionStorage,
 *     IndexedDB, cookies, console, or any cache.
 *   - cleared on every close path (challenge confirm, confirmed
 *     cancel, unmount).
 *   - the dialog uses aria-label, not aria-labelledby — see Modal.tsx
 *     for the rationale.
 *   - terminology discipline (ADR-0003 §"Consequences"): "recovery
 *     phrase" only — no "seed phrase", "mnemonic", "wallet", "BIP39",
 *     or "backup phrase" in code, copy, aria, or test names.
 */
import { useCallback, useState } from "react";

import { RecoveryPhraseConfirm } from "@/components/security/RecoveryPhraseConfirm";
import { Modal } from "@/components/ui/Modal";

const CANCEL_WARNING_TEXT =
  "This recovery phrase will not be shown again. If you close this now, " +
  "you may lose recovery ability for this installation's broker " +
  "credentials unless you have backed up the app data directory.";

type PhraseModalView = "confirm" | "confirm-cancel";

export interface RecoveryPhraseModalHandle {
  /** True iff the modal is currently open with a phrase. */
  readonly isOpen: boolean;
  /**
   * Imperative open with the phrase to display. The caller passes the
   * value straight from `createBrokerCredential`'s response — no
   * intermediate persistence. The hook holds it in component state
   * and discards it on every close path.
   */
  readonly open: (phrase: readonly string[]) => void;
  /** The JSX element to render somewhere stable in the caller's tree. */
  readonly element: JSX.Element;
}

export interface UseRecoveryPhraseModalOptions {
  /**
   * Called exactly once per close, regardless of which path closed
   * the modal (challenge confirm OR "Close anyway" from the cancel
   * gate). Used by callers for post-close actions like refreshing a
   * list or navigating to a destination route. Must NOT be used to
   * gate whether the modal closes — the modal always closes once
   * this fires.
   */
  readonly onClose: () => void;
}

export function useRecoveryPhraseModal(
  options: UseRecoveryPhraseModalOptions,
): RecoveryPhraseModalHandle {
  const { onClose } = options;
  const [phrase, setPhrase] = useState<readonly string[] | null>(null);
  const [view, setView] = useState<PhraseModalView>("confirm");

  const close = useCallback((): void => {
    // Single shared close path for both the challenge-confirm success
    // and the "Close anyway" branch (#121 round 2 review).
    // Responsibilities:
    //   - drop the phrase from state (it lives nowhere else)
    //   - reset the inner view back to "confirm" so the next open
    //     starts on the phrase, not on the warning
    //   - hand control back to the caller via onClose so it can run
    //     its own post-close action (refresh, navigate, ...)
    setPhrase(null);
    setView("confirm");
    onClose();
  }, [onClose]);

  const requestDismiss = useCallback((): void => {
    // Routed from RecoveryPhraseConfirm.onCancel AND from the Modal's
    // Escape handler. Both go through the confirm-cancel gate -- a
    // single misclick or stray Escape must not destroy the only copy
    // of the phrase.
    setView("confirm-cancel");
  }, []);

  const goBack = useCallback((): void => {
    setView("confirm");
  }, []);

  const open = useCallback((next: readonly string[]): void => {
    setPhrase(next);
    setView("confirm");
  }, []);

  const element = (
    <Modal
      isOpen={phrase !== null}
      onRequestClose={requestDismiss}
      label="Recovery phrase confirmation"
    >
      {/*
        Both inner branches guard on `phrase !== null` even though
        the Modal is already gated on `isOpen={phrase !== null}`. The
        confirm branch needs the guard for TypeScript narrowing
        (RecoveryPhraseConfirm's `phrase` prop is required and
        non-nullable). The cancel branch carries the same guard so
        the "this view never renders without a phrase" invariant is
        visible on both sides.
      */}
      {phrase !== null && view === "confirm" ? (
        <RecoveryPhraseConfirm
          phrase={phrase}
          onConfirmed={close}
          onCancel={requestDismiss}
        />
      ) : null}
      {phrase !== null && view === "confirm-cancel" ? (
        <div className="flex flex-col gap-4">
          <h2 className="text-sm font-semibold text-slate-700">
            Close without confirming?
          </h2>
          <p className="text-xs text-slate-600">{CANCEL_WARNING_TEXT}</p>
          <div className="flex justify-end gap-2">
            <button
              type="button"
              onClick={goBack}
              className="rounded bg-slate-800 px-3 py-1.5 text-xs font-medium text-white hover:bg-slate-700"
            >
              Go back
            </button>
            <button
              type="button"
              onClick={close}
              className="rounded border border-rose-300 bg-white dark:bg-slate-900 px-3 py-1.5 text-xs font-medium text-rose-700 hover:bg-rose-50"
            >
              Close anyway
            </button>
          </div>
        </div>
      ) : null}
    </Modal>
  );

  return { isOpen: phrase !== null, open, element };
}
