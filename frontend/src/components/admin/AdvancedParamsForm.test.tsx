/**
 * AdvancedParamsForm — render + submit per ParamFieldType (#1064 PR2).
 */

import { fireEvent, render, screen } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import type { ParamMetadata } from "@/api/types";
import { AdvancedParamsForm } from "@/components/admin/AdvancedParamsForm";

function meta(overrides: Partial<ParamMetadata>): ParamMetadata {
  return {
    name: overrides.name ?? "f",
    label: overrides.label ?? "Field",
    help_text: overrides.help_text ?? "",
    field_type: overrides.field_type ?? "string",
    default: overrides.default ?? null,
    advanced_group: overrides.advanced_group ?? true,
    enum_values: overrides.enum_values ?? null,
    min_value: overrides.min_value ?? null,
    max_value: overrides.max_value ?? null,
  };
}

function renderForm(metadata: ParamMetadata[]) {
  const onSubmit = vi.fn().mockResolvedValue(undefined);
  render(
    <AdvancedParamsForm metadata={metadata} busy={false} onSubmit={onSubmit} />,
  );
  return { onSubmit };
}

describe("AdvancedParamsForm", () => {
  it("renders empty-state when metadata is empty", () => {
    render(
      <AdvancedParamsForm metadata={[]} busy={false} onSubmit={vi.fn()} />,
    );
    expect(
      screen.getByText(/declares no operator-exposable params/i),
    ).toBeTruthy();
  });

  it("string: text input + submits string value", async () => {
    const { onSubmit } = renderForm([
      meta({ name: "label", field_type: "string", label: "Label" }),
    ]);
    const input = screen.getByLabelText("Label") as HTMLInputElement;
    expect(input.type).toBe("text");
    fireEvent.change(input, { target: { value: "hello" } });
    fireEvent.click(screen.getByRole("button"));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ label: "hello" });
  });

  it("int: number input + submits Number value", async () => {
    const { onSubmit } = renderForm([
      meta({ name: "n", field_type: "int", label: "N" }),
    ]);
    const input = screen.getByLabelText("N") as HTMLInputElement;
    expect(input.type).toBe("number");
    expect(input.step).toBe("1");
    fireEvent.change(input, { target: { value: "42" } });
    fireEvent.click(screen.getByRole("button"));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ n: 42 });
  });

  it("float: step=any + submits float", async () => {
    const { onSubmit } = renderForm([
      meta({ name: "x", field_type: "float", label: "X" }),
    ]);
    const input = screen.getByLabelText("X") as HTMLInputElement;
    expect(input.step).toBe("any");
    fireEvent.change(input, { target: { value: "3.14" } });
    fireEvent.click(screen.getByRole("button"));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ x: 3.14 });
  });

  it("date: date input + submits ISO string", async () => {
    const { onSubmit } = renderForm([
      meta({ name: "d", field_type: "date", label: "D" }),
    ]);
    const input = screen.getByLabelText("D") as HTMLInputElement;
    expect(input.type).toBe("date");
    fireEvent.change(input, { target: { value: "2024-01-01" } });
    fireEvent.click(screen.getByRole("button"));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ d: "2024-01-01" });
  });

  it("quarter: text input with pattern + submits string", async () => {
    const { onSubmit } = renderForm([
      meta({ name: "q", field_type: "quarter", label: "Q" }),
    ]);
    const input = screen.getByLabelText("Q") as HTMLInputElement;
    expect(input.pattern).toBe("\\d{4}Q[1-4]");
    fireEvent.change(input, { target: { value: "2026Q1" } });
    fireEvent.click(screen.getByRole("button"));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ q: "2026Q1" });
  });

  it("ticker: number input + submits Number (BE coerces to instrument_id)", async () => {
    const { onSubmit } = renderForm([
      meta({ name: "t", field_type: "ticker", label: "T" }),
    ]);
    const input = screen.getByLabelText("T") as HTMLInputElement;
    expect(input.type).toBe("number");
    fireEvent.change(input, { target: { value: "12345" } });
    fireEvent.click(screen.getByRole("button"));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ t: 12345 });
  });

  it("cik: text input + lets BE pad", async () => {
    const { onSubmit } = renderForm([
      meta({ name: "c", field_type: "cik", label: "C" }),
    ]);
    const input = screen.getByLabelText("C") as HTMLInputElement;
    expect(input.type).toBe("text");
    expect(input.inputMode).toBe("numeric");
    fireEvent.change(input, { target: { value: "320193" } });
    fireEvent.click(screen.getByRole("button"));
    await Promise.resolve();
    // FE submits raw digits; BE _coerce_value zfill(10) at validation.
    expect(onSubmit).toHaveBeenCalledWith({ c: "320193" });
  });

  it("bool: checkbox always submits even when unchecked", async () => {
    const { onSubmit } = renderForm([
      meta({ name: "b", field_type: "bool", label: "B", default: false }),
    ]);
    fireEvent.click(screen.getByRole("button"));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ b: false });
  });

  it("bool: checked submits true", async () => {
    const { onSubmit } = renderForm([
      meta({ name: "b", field_type: "bool", label: "B", default: false }),
    ]);
    fireEvent.click(screen.getByLabelText("B"));
    fireEvent.click(screen.getByRole("button", { name: /run/i }));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ b: true });
  });

  it("enum: select restricted to enum_values", async () => {
    const { onSubmit } = renderForm([
      meta({
        name: "e",
        field_type: "enum",
        label: "E",
        enum_values: ["a", "b", "c"],
      }),
    ]);
    const select = screen.getByLabelText("E") as HTMLSelectElement;
    expect(select.tagName).toBe("SELECT");
    expect(Array.from(select.options).map((o) => o.value)).toEqual([
      "",
      "a",
      "b",
      "c",
    ]);
    fireEvent.change(select, { target: { value: "b" } });
    fireEvent.click(screen.getByRole("button"));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ e: "b" });
  });

  it("multi_enum: checkbox list submits string[]", async () => {
    const { onSubmit } = renderForm([
      meta({
        name: "m",
        field_type: "multi_enum",
        label: "M",
        enum_values: ["x", "y", "z"],
      }),
    ]);
    fireEvent.click(screen.getByLabelText("x"));
    fireEvent.click(screen.getByLabelText("z"));
    fireEvent.click(screen.getByRole("button"));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ m: ["x", "z"] });
  });

  it("multi_enum: group's aria-labelledby targets a real element id", () => {
    // Review-bot WARNING from PR #1100 — the group div previously
    // referenced ``id`` (the input id, never assigned to anything),
    // breaking SR association. Pin: the referenced id resolves to a
    // rendered <label> in the document.
    render(
      <AdvancedParamsForm
        metadata={[
          meta({
            name: "m",
            field_type: "multi_enum",
            label: "M",
            enum_values: ["a"],
          }),
        ]}
        busy={false}
        onSubmit={vi.fn()}
      />,
    );
    const group = screen.getByRole("group");
    const labelledBy = group.getAttribute("aria-labelledby");
    expect(labelledBy).toBeTruthy();
    expect(document.getElementById(labelledBy!)?.textContent).toBe("M");
  });

  it("empty optional string fields are omitted from submit", async () => {
    const { onSubmit } = renderForm([
      meta({ name: "left_blank", field_type: "string", label: "Blank" }),
      meta({ name: "filled", field_type: "string", label: "Filled" }),
    ]);
    fireEvent.change(screen.getByLabelText("Filled"), {
      target: { value: "x" },
    });
    fireEvent.click(screen.getByRole("button", { name: /run/i }));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({ filled: "x" });
  });

  it("empty number input is omitted from submit (HTML5 filters non-numeric)", async () => {
    // HTML5 number inputs reject non-numeric keystrokes at the
    // browser layer — the form state stays at the initial empty
    // string. isLocalEmpty drops empty strings from the submit
    // payload, so the BE never sees a NaN/Infinity for these fields.
    // The defensive Number.isFinite guard inside coerceForSubmit is
    // belt-and-braces for programmatic values that bypass the input.
    const { onSubmit } = renderForm([
      meta({ name: "n", field_type: "int", label: "N" }),
    ]);
    fireEvent.click(screen.getByRole("button", { name: /run/i }));
    await Promise.resolve();
    expect(onSubmit).toHaveBeenCalledWith({});
  });

  it("enum with missing enum_values falls back to text input + warns", () => {
    const warn = vi
      .spyOn(console, "warn")
      .mockImplementation(() => undefined);
    render(
      <AdvancedParamsForm
        metadata={[
          meta({
            name: "broken",
            field_type: "enum",
            label: "Broken",
            enum_values: null,
          }),
        ]}
        busy={false}
        onSubmit={vi.fn()}
      />,
    );
    const fallback = screen.getByLabelText("Broken") as HTMLInputElement;
    expect(fallback.type).toBe("text");
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });

  it("disables submit button when busy", () => {
    render(
      <AdvancedParamsForm
        metadata={[meta({ name: "f", field_type: "string", label: "F" })]}
        busy={true}
        onSubmit={vi.fn()}
      />,
    );
    const btn = screen.getByRole("button", { name: /run/i }) as HTMLButtonElement;
    expect(btn.disabled).toBe(true);
  });

  it("seeds string input with default value when provided", () => {
    render(
      <AdvancedParamsForm
        metadata={[
          meta({
            name: "f",
            field_type: "string",
            label: "F",
            default: "seed",
          }),
        ]}
        busy={false}
        onSubmit={vi.fn()}
      />,
    );
    expect((screen.getByLabelText("F") as HTMLInputElement).value).toBe(
      "seed",
    );
  });
});
