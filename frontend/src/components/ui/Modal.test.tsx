/**
 * Tests for Modal — reusable dialog primitive (#121).
 *
 * Covers the contract documented in Modal.tsx:
 *   - role="dialog" + aria-modal + aria-labelledby wired
 *   - focus moves to first tabbable on open, restored on close
 *   - Tab and Shift+Tab cycle within the dialog (focus trap)
 *   - Escape routes through onRequestClose (NOT auto-dismiss)
 *   - overlay is non-interactive (no click-through dismissal)
 *   - the component never closes itself; only the caller toggles isOpen
 */
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { Modal } from "@/components/ui/Modal";

function Harness({
  initiallyOpen = true,
  onRequestClose,
}: {
  initiallyOpen?: boolean;
  onRequestClose?: () => void;
}): JSX.Element {
  const [open, setOpen] = useState(initiallyOpen);
  return (
    <>
      <button type="button" onClick={() => setOpen(true)}>
        open
      </button>
      <button type="button" onClick={() => setOpen(false)}>
        external close
      </button>
      <Modal
        isOpen={open}
        onRequestClose={onRequestClose ?? (() => setOpen(false))}
        labelledBy="modal-heading"
      >
        <h2 id="modal-heading">Modal title</h2>
        <button type="button">first</button>
        <button type="button">second</button>
        <button type="button">third</button>
      </Modal>
    </>
  );
}

describe("Modal — accessibility wiring", () => {
  it("renders nothing when isOpen is false", () => {
    render(<Harness initiallyOpen={false} />);
    expect(screen.queryByRole("dialog")).toBeNull();
  });

  it("exposes role=dialog, aria-modal=true, aria-labelledby on open", () => {
    render(<Harness />);
    const dialog = screen.getByRole("dialog");
    expect(dialog).toHaveAttribute("aria-modal", "true");
    expect(dialog).toHaveAttribute("aria-labelledby", "modal-heading");
  });

  it("supports aria-label as an alternative to aria-labelledby", () => {
    render(
      <Modal
        isOpen={true}
        onRequestClose={vi.fn()}
        label="Recovery phrase confirmation"
      >
        <button type="button">first</button>
      </Modal>,
    );
    const dialog = screen.getByRole("dialog");
    expect(dialog).toHaveAttribute("aria-label", "Recovery phrase confirmation");
    expect(dialog).not.toHaveAttribute("aria-labelledby");
    // The dialog is discoverable by its accessible name.
    expect(
      screen.getByRole("dialog", { name: "Recovery phrase confirmation" }),
    ).toBeInTheDocument();
  });
});

describe("Modal — focus management", () => {
  it("moves focus to the first tabbable child on open", async () => {
    render(<Harness />);
    expect(screen.getByRole("button", { name: "first" })).toHaveFocus();
  });

  it("restores focus to the previously focused element on close", async () => {
    const user = userEvent.setup();
    render(<Harness initiallyOpen={false} />);
    const opener = screen.getByRole("button", { name: "open" });
    opener.focus();
    expect(opener).toHaveFocus();
    await user.click(opener);
    expect(screen.getByRole("button", { name: "first" })).toHaveFocus();
    await user.click(screen.getByRole("button", { name: "external close" }));
    expect(opener).toHaveFocus();
  });

  it("traps Tab forward from the last tabbable to the first", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    screen.getByRole("button", { name: "third" }).focus();
    await user.tab();
    expect(screen.getByRole("button", { name: "first" })).toHaveFocus();
  });

  it("traps Shift+Tab backward from the first tabbable to the last", async () => {
    const user = userEvent.setup();
    render(<Harness />);
    expect(screen.getByRole("button", { name: "first" })).toHaveFocus();
    await user.tab({ shift: true });
    expect(screen.getByRole("button", { name: "third" })).toHaveFocus();
  });
});

describe("Modal — focus capture race", () => {
  it("restores focus to the original trigger across a rapid open/close/open cycle", async () => {
    // Regression for PR #125 round 1 review: a rapid isOpen toggle
    // must not cause `previouslyFocusedRef` to capture a node INSIDE
    // the dialog. The capture is now guarded so it only fires when
    // document.activeElement is outside the dialog.
    const user = userEvent.setup();
    function ToggleHarness(): JSX.Element {
      const [open, setOpen] = useState(false);
      return (
        <>
          <button type="button" onClick={() => setOpen(true)}>
            trigger
          </button>
          <button type="button" onClick={() => setOpen(false)}>
            close
          </button>
          <Modal
            isOpen={open}
            onRequestClose={() => setOpen(false)}
            labelledBy="h"
          >
            <h2 id="h">title</h2>
            <button type="button">inside</button>
          </Modal>
        </>
      );
    }
    render(<ToggleHarness />);
    const trigger = screen.getByRole("button", { name: "trigger" });
    trigger.focus();
    await user.click(trigger); // open
    expect(screen.getByRole("button", { name: "inside" })).toHaveFocus();
    await user.click(screen.getByRole("button", { name: "close" }));
    expect(trigger).toHaveFocus();
    // Re-open and re-close — focus must still return to the trigger,
    // not to the "inside" button captured during the previous open.
    await user.click(trigger);
    await user.click(screen.getByRole("button", { name: "close" }));
    expect(trigger).toHaveFocus();
  });
});

describe("Modal — dismissal contract", () => {
  it("routes Escape through onRequestClose without closing itself", async () => {
    const user = userEvent.setup();
    const onRequestClose = vi.fn();
    render(<Harness onRequestClose={onRequestClose} />);
    await user.keyboard("{Escape}");
    expect(onRequestClose).toHaveBeenCalledTimes(1);
    // The component must not have unmounted itself.
    expect(screen.getByRole("dialog")).toBeInTheDocument();
  });

  it("does not dismiss on overlay click — overlay has no click handler", async () => {
    const user = userEvent.setup();
    const onRequestClose = vi.fn();
    render(<Harness onRequestClose={onRequestClose} />);
    // The overlay is the parent of the dialog. Clicking it must not
    // call onRequestClose. We click the dialog's parentElement to
    // exercise the actual overlay node.
    const dialog = screen.getByRole("dialog");
    const overlay = dialog.parentElement!;
    await user.click(overlay);
    expect(onRequestClose).not.toHaveBeenCalled();
  });
});
