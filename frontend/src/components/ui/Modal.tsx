/**
 * Modal — reusable dialog primitive (introduced for #121, reused by #122).
 *
 * This is the first modal in the frontend. The component owns:
 *
 *   - the fixed-position overlay
 *   - the dialog container with role="dialog" + aria-modal="true"
 *   - aria-labelledby wiring (caller supplies the heading id)
 *   - focus trap (Tab / Shift+Tab cycle within the dialog)
 *   - initial focus on first tabbable element on open
 *   - focus restoration to the previously-focused element on close
 *   - Escape key handling, routed through the caller's `onRequestClose`
 *
 * What it deliberately does NOT own:
 *
 *   - the close-confirmation gate. The recovery-phrase flow (#121, #122)
 *     is fail-closed: a misclick on Escape, the close button, or the
 *     overlay must NOT silently dismiss the dialog. Instead the caller
 *     funnels every dismissal attempt through `onRequestClose` and
 *     decides whether to actually close. This component never closes
 *     itself -- it just signals intent.
 *
 *   - overlay-click dismissal. ADR-0003 / #121 explicitly rules this
 *     out: a single misclick outside the dialog must not destroy a
 *     just-shown recovery phrase. The overlay is a backdrop only and
 *     does not respond to pointer events. Future non-fail-closed
 *     callers can opt in to dismiss-on-overlay-click via a future
 *     prop, but the default is "no".
 *
 * Focus trap: implemented manually (no `focus-trap-react` dep). The
 * trap walks the rendered subtree for tabbable elements on each Tab
 * keystroke rather than caching them, so dynamically-revealed inputs
 * (e.g. moving from the display stage to the challenge stage in
 * RecoveryPhraseConfirm) are picked up automatically.
 */
import { useCallback, useEffect, useId, useRef } from "react";
import type { KeyboardEvent, ReactNode } from "react";

const TABBABLE_SELECTOR = [
  "a[href]",
  "button:not([disabled])",
  "input:not([disabled]):not([type='hidden'])",
  "select:not([disabled])",
  "textarea:not([disabled])",
  "[tabindex]:not([tabindex='-1'])",
].join(",");

function getTabbables(root: HTMLElement): HTMLElement[] {
  // The selector itself already excludes disabled/hidden/tabindex=-1.
  // We deliberately do NOT use `offsetParent !== null` as a visibility
  // filter: jsdom does not compute layout, so offsetParent is always
  // null in unit tests. For the modal's actual use cases (a single
  // form's worth of inputs and buttons), the selector is enough.
  return Array.from(root.querySelectorAll<HTMLElement>(TABBABLE_SELECTOR)).filter(
    (el) => el.getAttribute("aria-hidden") !== "true",
  );
}

export interface ModalProps {
  readonly isOpen: boolean;
  /**
   * Called when the operator attempts to dismiss the modal via Escape
   * or any caller-rendered close affordance. NEVER called from an
   * overlay click — the overlay is non-interactive by design (see file
   * header). The caller decides whether to actually close; this
   * component never unmounts itself.
   */
  readonly onRequestClose: () => void;
  /**
   * The id of the heading element inside `children`. Wired into
   * aria-labelledby on the dialog container so screen readers announce
   * the modal title on open. The caller is responsible for rendering
   * an element with this id.
   */
  readonly labelledBy: string;
  readonly children: ReactNode;
}

export function Modal({
  isOpen,
  onRequestClose,
  labelledBy,
  children,
}: ModalProps): JSX.Element | null {
  const dialogRef = useRef<HTMLDivElement | null>(null);
  // Element that had focus before the modal opened. Restored on close
  // so keyboard users land back on the trigger button rather than the
  // top of <body>.
  const previouslyFocusedRef = useRef<HTMLElement | null>(null);

  useEffect(() => {
    if (!isOpen) return;
    previouslyFocusedRef.current =
      (document.activeElement as HTMLElement | null) ?? null;
    // Focus the first tabbable element on open. Defer to the next tick
    // so children that mount tabbables in their own effects (e.g. an
    // input with autoFocus) settle first.
    const dialog = dialogRef.current;
    if (dialog !== null) {
      const tabbables = getTabbables(dialog);
      if (tabbables.length > 0) {
        tabbables[0]!.focus();
      } else {
        // Fall back to focusing the dialog itself so the trap has
        // something to anchor on.
        dialog.focus();
      }
    }
    return () => {
      const prev = previouslyFocusedRef.current;
      if (prev !== null && typeof prev.focus === "function") {
        prev.focus();
      }
    };
  }, [isOpen]);

  const handleKeyDown = useCallback(
    (event: KeyboardEvent<HTMLDivElement>) => {
      if (event.key === "Escape") {
        // Route through the caller. Critically, do NOT close ourselves
        // -- the recovery-phrase flow turns Escape into a confirm-cancel
        // gate, and silently dismissing here would defeat that.
        event.stopPropagation();
        onRequestClose();
        return;
      }
      if (event.key !== "Tab") return;
      const dialog = dialogRef.current;
      if (dialog === null) return;
      const tabbables = getTabbables(dialog);
      if (tabbables.length === 0) {
        event.preventDefault();
        return;
      }
      const first = tabbables[0]!;
      const last = tabbables[tabbables.length - 1]!;
      const active = document.activeElement;
      if (event.shiftKey) {
        if (active === first || !dialog.contains(active)) {
          event.preventDefault();
          last.focus();
        }
      } else {
        if (active === last || !dialog.contains(active)) {
          event.preventDefault();
          first.focus();
        }
      }
    },
    [onRequestClose],
  );

  if (!isOpen) return null;

  return (
    <div
      // The overlay is a backdrop only — no onClick handler. See file
      // header for the rationale (fail-closed flow, no silent dismiss).
      className="fixed inset-0 z-50 flex items-center justify-center bg-slate-900/60 p-4"
      aria-hidden="false"
    >
      <div
        ref={dialogRef}
        role="dialog"
        aria-modal="true"
        aria-labelledby={labelledBy}
        tabIndex={-1}
        onKeyDown={handleKeyDown}
        className="max-h-[90vh] w-full max-w-md overflow-y-auto rounded-lg bg-white p-5 shadow-xl outline-none"
      >
        {children}
      </div>
    </div>
  );
}

/**
 * Convenience hook for callers that need to mint a stable id for the
 * heading element they pass to `labelledBy`. Re-exports React's
 * `useId` so call sites don't have to import it separately and so the
 * intent ("this id is for an aria-labelledby wire") is grep-able.
 */
export function useModalHeadingId(): string {
  return useId();
}
