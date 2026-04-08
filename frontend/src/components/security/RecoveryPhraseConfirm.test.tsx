/**
 * Tests for RecoveryPhraseConfirm — ADR-0003 Ticket 2a (#120).
 *
 * The challenge indices are picked from `crypto.getRandomValues` once
 * per component instance and are deliberately not exposed as a prop, so
 * tests cannot pin them directly. The tests work around this by reading
 * the rendered "Word #N" labels off the DOM and looking up the matching
 * fixture word — the same path a real operator takes. This keeps the
 * assertions behavioural (no monkey-patching of the crypto primitive)
 * and exercises the user-visible flow end-to-end.
 *
 * Test-name discipline (ADR-0003 §"Consequences"): only "recovery
 * phrase" — never "seed phrase", "mnemonic", "wallet", "BIP39", or
 * "backup phrase".
 */
import { useState } from "react";
import { describe, expect, it, vi } from "vitest";
import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

import { RecoveryPhraseConfirm } from "@/components/security/RecoveryPhraseConfirm";

const VALID_PHRASE: readonly string[] = [
  "alpha",
  "bravo",
  "charlie",
  "delta",
  "echo",
  "foxtrot",
  "golf",
  "hotel",
  "india",
  "juliet",
  "kilo",
  "lima",
  "mike",
  "november",
  "oscar",
  "papa",
  "quebec",
  "romeo",
  "sierra",
  "tango",
  "uniform",
  "victor",
  "whiskey",
  "xray",
];

interface ChallengeSlot {
  readonly position: number;
  readonly input: HTMLInputElement;
}

function getChallengeSlots(): ChallengeSlot[] {
  const labels = screen.getAllByText(/^Word #\d+$/);
  return labels.map((labelText) => {
    const labelEl = labelText.closest("label");
    if (labelEl === null) {
      throw new Error("Word label not inside a <label> element");
    }
    const input = within(labelEl).getByRole("textbox") as HTMLInputElement;
    const match = /Word #(\d+)/.exec(labelText.textContent ?? "");
    if (match === null) {
      throw new Error(`Could not parse word position from label: ${labelText.textContent}`);
    }
    return { position: Number.parseInt(match[1]!, 10), input };
  });
}

async function advancePastWrittenDownGate(): Promise<void> {
  const user = userEvent.setup();
  await user.click(screen.getByLabelText(/I have written down/i));
  await user.click(screen.getByRole("button", { name: "Continue" }));
}

describe("RecoveryPhraseConfirm — display stage", () => {
  it("renders all 24 numbered words from the supplied recovery phrase", () => {
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE}
        onConfirmed={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    const list = screen.getByRole("list", { name: "Recovery phrase words" });
    const items = within(list).getAllByRole("listitem");
    expect(items).toHaveLength(24);
    expect(items[0]).toHaveTextContent("1.");
    expect(items[0]).toHaveTextContent("alpha");
    expect(items[12]).toHaveTextContent("13.");
    expect(items[12]).toHaveTextContent("mike");
    expect(items[23]).toHaveTextContent("24.");
    expect(items[23]).toHaveTextContent("xray");
  });

  it("disables Continue until the operator confirms they have written it down", async () => {
    const user = userEvent.setup();
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE}
        onConfirmed={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    const continueButton = screen.getByRole("button", { name: "Continue" });
    expect(continueButton).toBeDisabled();

    await user.click(screen.getByLabelText(/I have written down/i));
    expect(continueButton).toBeEnabled();
  });

  it("calls onCancel and never onConfirmed when cancelled from the display stage", async () => {
    const user = userEvent.setup();
    const onCancel = vi.fn();
    const onConfirmed = vi.fn();
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE}
        onConfirmed={onConfirmed}
        onCancel={onCancel}
      />,
    );

    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(onCancel).toHaveBeenCalledTimes(1);
    expect(onConfirmed).not.toHaveBeenCalled();
  });
});

describe("RecoveryPhraseConfirm — challenge stage", () => {
  it("calls onConfirmed only after every challenge slot matches the recovery phrase", async () => {
    const user = userEvent.setup();
    const onConfirmed = vi.fn();
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE}
        onConfirmed={onConfirmed}
        onCancel={vi.fn()}
      />,
    );

    await advancePastWrittenDownGate();

    const slots = getChallengeSlots();
    expect(slots).toHaveLength(3);

    for (const { position, input } of slots) {
      await user.type(input, VALID_PHRASE[position - 1]!);
    }
    await user.click(screen.getByRole("button", { name: "Confirm" }));
    expect(onConfirmed).toHaveBeenCalledTimes(1);
  });

  it("accepts case-insensitive trimmed input as a correct answer", async () => {
    const user = userEvent.setup();
    const onConfirmed = vi.fn();
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE}
        onConfirmed={onConfirmed}
        onCancel={vi.fn()}
      />,
    );

    await advancePastWrittenDownGate();

    for (const { position, input } of getChallengeSlots()) {
      await user.type(input, `  ${VALID_PHRASE[position - 1]!.toUpperCase()}  `);
    }
    await user.click(screen.getByRole("button", { name: "Confirm" }));
    expect(onConfirmed).toHaveBeenCalledTimes(1);
  });

  it("shows a fixed-phrase inline error and does not call onConfirmed when answers are wrong", async () => {
    const user = userEvent.setup();
    const onConfirmed = vi.fn();
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE}
        onConfirmed={onConfirmed}
        onCancel={vi.fn()}
      />,
    );

    await advancePastWrittenDownGate();

    for (const { input } of getChallengeSlots()) {
      await user.type(input, "wrong");
    }
    await user.click(screen.getByRole("button", { name: "Confirm" }));

    expect(screen.getByRole("alert")).toHaveTextContent(
      /do not match your recovery phrase/i,
    );
    expect(onConfirmed).not.toHaveBeenCalled();
  });

  it("clears the inline error as soon as the operator edits a slot", async () => {
    const user = userEvent.setup();
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE}
        onConfirmed={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    await advancePastWrittenDownGate();
    const slots = getChallengeSlots();
    for (const { input } of slots) {
      await user.type(input, "wrong");
    }
    await user.click(screen.getByRole("button", { name: "Confirm" }));
    expect(screen.queryByRole("alert")).not.toBeNull();

    await user.type(slots[0]!.input, "x");
    expect(screen.queryByRole("alert")).toBeNull();
  });

  it("does not regenerate the challenge positions on parent re-render", async () => {
    const user = userEvent.setup();
    function Wrapper(): JSX.Element {
      const [, setN] = useState(0);
      return (
        <>
          <button type="button" onClick={() => setN((x) => x + 1)}>
            force
          </button>
          <RecoveryPhraseConfirm
            phrase={VALID_PHRASE}
            onConfirmed={vi.fn()}
            onCancel={vi.fn()}
          />
        </>
      );
    }
    render(<Wrapper />);
    await advancePastWrittenDownGate();

    const before = getChallengeSlots().map((s) => s.position);
    await user.click(screen.getByRole("button", { name: "force" }));
    const after = getChallengeSlots().map((s) => s.position);
    expect(after).toEqual(before);
  });

  it("calls onCancel and never onConfirmed when cancelled mid-challenge", async () => {
    const user = userEvent.setup();
    const onCancel = vi.fn();
    const onConfirmed = vi.fn();
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE}
        onConfirmed={onConfirmed}
        onCancel={onCancel}
      />,
    );

    await advancePastWrittenDownGate();
    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(onCancel).toHaveBeenCalledTimes(1);
    expect(onConfirmed).not.toHaveBeenCalled();
  });

  it("respects challengeCount as a test seam", async () => {
    const user = userEvent.setup();
    const onConfirmed = vi.fn();
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE}
        onConfirmed={onConfirmed}
        onCancel={vi.fn()}
        challengeCount={1}
      />,
    );

    await advancePastWrittenDownGate();
    const slots = getChallengeSlots();
    expect(slots).toHaveLength(1);
    await user.type(slots[0]!.input, VALID_PHRASE[slots[0]!.position - 1]!);
    await user.click(screen.getByRole("button", { name: "Confirm" }));
    expect(onConfirmed).toHaveBeenCalledTimes(1);
  });
});

describe("RecoveryPhraseConfirm — fail-closed shapes", () => {
  it("renders the unavailable state when the recovery phrase is the wrong length", async () => {
    const user = userEvent.setup();
    const onCancel = vi.fn();
    const onConfirmed = vi.fn();
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE.slice(0, 23)}
        onConfirmed={onConfirmed}
        onCancel={onCancel}
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent(
      /Recovery phrase unavailable/i,
    );
    expect(screen.queryByRole("button", { name: "Continue" })).toBeNull();

    await user.click(screen.getByRole("button", { name: "Cancel" }));
    expect(onCancel).toHaveBeenCalledTimes(1);
    expect(onConfirmed).not.toHaveBeenCalled();
  });

  it("renders the unavailable state when any word in the recovery phrase is empty", () => {
    const phraseWithEmpty = VALID_PHRASE.map((w, i) => (i === 5 ? "  " : w));
    render(
      <RecoveryPhraseConfirm
        phrase={phraseWithEmpty}
        onConfirmed={vi.fn()}
        onCancel={vi.fn()}
      />,
    );

    expect(screen.getByRole("alert")).toHaveTextContent(
      /Recovery phrase unavailable/i,
    );
  });

  it("never persists the recovery phrase to localStorage or sessionStorage", async () => {
    const user = userEvent.setup();
    const setItemSpy = vi.spyOn(Storage.prototype, "setItem");
    render(
      <RecoveryPhraseConfirm
        phrase={VALID_PHRASE}
        onConfirmed={vi.fn()}
        onCancel={vi.fn()}
      />,
    );
    await advancePastWrittenDownGate();
    for (const { position, input } of getChallengeSlots()) {
      await user.type(input, VALID_PHRASE[position - 1]!);
    }
    await user.click(screen.getByRole("button", { name: "Confirm" }));
    expect(setItemSpy).not.toHaveBeenCalled();
    setItemSpy.mockRestore();
  });
});
